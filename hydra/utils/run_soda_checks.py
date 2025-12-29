# Databricks notebook source

from pyspark.sql import SparkSession
from soda.scan import Scan
from datetime import date, datetime
import json
import yaml
import os
import time
import re
from sqlglot import exp, parse_one

# COMMAND ----------

# ---------------------------------------
# ✅ Initialize Spark Session
# ---------------------------------------
spark = SparkSession.builder.appName("SODA_CHECKS").getOrCreate()

# ---------------------------------------
# ✅ Load Widgets and Config
# ---------------------------------------
try:
    config_path = dbutils.widgets.get("soda_config_path")
except:
    dbutils.widgets.text("soda_config_path", "../tests/config/default.yaml")
    config_path = dbutils.widgets.get("soda_config_path")

try:
    input_param_str = dbutils.widgets.get("inputParam")
except:
    dbutils.widgets.text("inputParam", '{"environment": "local"}')
    input_param_str = dbutils.widgets.get("inputParam")

input_param = json.loads(input_param_str)
environment = input_param.get("environment", "_dev")

if not os.path.exists(config_path):
    raise FileNotFoundError(f"❌ Config file not found at: {config_path}")

_NUMBER_ONLY = re.compile(r'^\s*-?\d+(?:\.\d+)?\s*$')

def quote_operator_thresholds(raw_text: str) -> str:
    """
    Quote warn/fail values that include comparison operators or BETWEEN so yaml.safe_load
    doesn't treat them as YAML tags (e.g., '!= 0' or '> 0').

    Handles lines like:
      warn: > 0
      warn: != 1
      warn: between 1 and 3
      warn: not between 0 and 0
      fail: <= 5
      - warn: <> 0     # in lists too

    Leaves as-is:
      warn: 10
      warn: "when > 0"
      warn: 'between 1 and 3'
    """
    out_lines = []
    for line in raw_text.splitlines():
        m = re.match(r'^(\s*(?:- )?(warn|fail)\s*:\s*)([^#\n]*?)(\s*(?:#.*)?)$', line)
        if not m:
            out_lines.append(line)
            continue

        pre, _key, val, post = m.groups()
        v = (val or '').strip()

        if not v or (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
            out_lines.append(line)
            continue

        if _NUMBER_ONLY.match(v):
            out_lines.append(line)
            continue

        vl = v.lower()
        if vl.startswith('when '):
            out_lines.append(line)
            continue

        needs_quote = (
            vl.startswith(('>', '<', '=', '!')) or
            ' between ' in f' {vl} ' or
            ' not between ' in f' {vl} ' or
            '<>' in v or '!=' in v
        )
        out_lines.append(f'{pre}"{v}"{post or ""}' if needs_quote else line)

    return "\n".join(out_lines)

with open(config_path, 'r') as f:
    _raw_yaml = f.read()

_raw_yaml = _raw_yaml.replace("{env}", environment)  # keep existing {env} pattern
_raw_yaml = quote_operator_thresholds(_raw_yaml)
config = yaml.safe_load(_raw_yaml)


# ---------------------------------------
# ✅ Load Soda Data Source Mapping YAML
# ---------------------------------------
data_source_mapping_path = "../utils/soda-data-source-mapping.yml"
if not os.path.exists(data_source_mapping_path):
    raise FileNotFoundError(f"❌ Soda Data Source Mapping file not found at: {data_source_mapping_path}")

with open(data_source_mapping_path, 'r') as f:
    data_source_mapping = yaml.safe_load(f)

# ---------------------------------------
# ✅ Generate Date Filter
# ---------------------------------------
def get_date_filter_condition(date_config, environment):
    col = date_config.get('date_column', 'dw_insert_ts')
    mode = date_config.get('mode', 'today')
    from_date = date_config.get('from_date')
    to_date = date_config.get('to_date')

    if environment.lower() == '_prod' or mode == 'today':
        today_str = date.today().isoformat()
        return f"to_date({col}) = '{today_str}'"
    elif mode == 'single':
        if not from_date:
            raise ValueError("from_date is required for single mode")
        return f"to_date({col}) = '{from_date}'"
    elif mode == 'range':
        if not from_date or not to_date:
            raise ValueError("from_date and to_date are required for range mode")
        return f"to_date({col}) BETWEEN '{from_date}' AND '{to_date}'"
    else:
        raise ValueError(f"Invalid mode: {mode}")

date_filter_condition = get_date_filter_condition(config.get("date_config", {}), environment)

# ---------------------------------------
# ✅ Build Checks YAML
# ---------------------------------------
_OPERATORS_RE = r"(=|<=|>=|<>|!=|<|>)"
_BETWEEN_RE = re.compile(r"(?i)^\s*(not\s+between|between)\s+(-?\d+(?:\.\d+)?)\s+and\s+(-?\d+(?:\.\d+)?)\s*$")
_SIMPLE_RE = re.compile(rf"(?i)^\s*(?:when\s+)?{_OPERATORS_RE}\s*(-?\d+(?:\.\d+)?)\s*$")
_NUM_RE = re.compile(r"^\s*-?\d+(?:\.\d+)?\s*$")

def normalize_threshold(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return f"when > {v}"
    s = str(v).strip()
    m = _BETWEEN_RE.match(s)
    if m:
        kw, a, b = m.groups()
        return f"when {kw.lower()} {a} and {b}"
    m = _SIMPLE_RE.match(s)
    if m:
        op, num = m.groups()
        return f"when {op} {num}"
    if _NUM_RE.match(s):
        return f"when > {s}"
    if s.lower().startswith("when "):
        return s
    raise ValueError(f"Unrecognized threshold format: {v!r}")

def ensure_final_metric_alias(sql: str) -> str:
    """
    Ensure the outer SELECT (after optional WITH) projects exactly one column named 'metric'.
    Disallow SELECT * in the outer projection.
    """
    try:
        tree = parse_one(sql, read="spark")
    except Exception as e:
        raise ValueError(f"Failed to parse custom_sql with sqlglot (Spark): {e}")

    def main_select(node):
        if isinstance(node, exp.With):
            target = node.this
            if isinstance(target, exp.Select):
                return target
            return target.find(exp.Select)
        if isinstance(node, exp.Select):
            return node
        return node.find(exp.Select)

    outer = main_select(tree)
    if not outer:
        raise ValueError("No outer SELECT found in custom_sql query.")

    for proj in outer.expressions:
        if isinstance(proj, exp.Star):
            raise ValueError("Final SELECT uses '*'. Please project a single expression aliased as 'metric'.")

    exprs = list(outer.expressions)

    def proj_alias_name(p):
        if isinstance(p, exp.Alias):
            return (p.alias_or_name or "").strip().lower()
        if isinstance(p, exp.Column):
            return (p.name or "").strip().lower()
        return ""

    if len(exprs) > 1:
        metric_idxs = [i for i, p in enumerate(exprs) if proj_alias_name(p) == "metric"]
        if len(metric_idxs) == 1:
            keep = exprs[metric_idxs[0]]
            outer.set("expressions", [keep])
            return tree.sql(dialect="spark")

        found = []
        for p in exprs:
            nm = proj_alias_name(p)
            found.append(nm if nm else p.sql(dialect="spark"))
        raise ValueError(
            "Final SELECT must output exactly one column for custom_sql metric, "
            "or include exactly one projection aliased as 'metric'. "
            f"Found: {', '.join(found)}"
        )

    proj = exprs[0]
    if proj_alias_name(proj) == "metric":
        return tree.sql(dialect="spark")

    if isinstance(proj, exp.Alias):
        proj.set("alias", exp.to_identifier("metric"))
    else:
        outer.set("expressions", [exp.alias_(proj.copy(), "metric")])

    return tree.sql(dialect="spark")

checks_yaml = "# Generated Soda Checks\n\n"

for dataset in config.get("datasets", []):
    dataset_name = dataset["name"].format(env=environment)
    filter_tag = "[soda_filter]"

    # 🔹 Add filter section with date logic
    checks_yaml += f"filter {dataset_name} {filter_tag}:\n"
    checks_yaml += f"  where: {date_filter_condition}\n\n"

    # 🔹 Add configurations for block
    dataset_attributes = dataset.get("attributes", {})
    if dataset_attributes:
        checks_yaml += f"configurations for {dataset_name}:\n"
        checks_yaml += "  attributes:\n"
        for key, value in dataset_attributes.items():
            checks_yaml += f"    {key}: {value}\n"

    # 🔹 Add checks section
    checks_yaml += f"\nchecks for {dataset_name} {filter_tag}:\n"

    # Extract name parts
    ds_parts = dataset_name.split('.')
    title_env = ds_parts[0]
    title_name, env_name = title_env.split('_')
    layer_name = ds_parts[1]
    table_name = ds_parts[2]

    def _validate_list(x):
        if isinstance(x, list) and all(isinstance(i, str) for i in x) and x:
            return x
        raise ValueError(f"Expected a non-empty flat list of strings, got: {x!r}")

    _custom_sql_entries = []
    required_columns_for_schema = []

    for check in dataset.get('checks', []):
        check_type = check.get('name')
        columns = check.get('columns', [])

        if check_type == 'row_count':
            check_name = f"row_count|{table_name}"
            checks_yaml += f"  - row_count > 0:\n"
            checks_yaml += f"      name: {check_name}\n"
            continue

        if check_type == 'custom_sql':
            raw_sql = (check.get("sql") or "").strip()
            if not raw_sql:
                continue

            # enforce single 'metric' column on the OUTER select
            metric_sql = ensure_final_metric_alias(raw_sql)

            warn_cond = normalize_threshold(check.get("warn")) if "warn" in check else None
            fail_cond = normalize_threshold(check.get("fail")) if "fail" in check else None
            if not warn_cond and not fail_cond:
                fail_cond = "when > 0"

            qn = check.get("query_name", "custom_sql")
            entry = f"  - metric:\n" \
                    f"      name: custom_sql|{table_name}|{qn}\n" \
                    f"      metric query: |\n" \
                    + "\n".join([f"        {ln}" for ln in metric_sql.splitlines()])
            if fail_cond:
                entry += f"\n      fail: {fail_cond}"
            if warn_cond:
                entry += f"\n      warn: {warn_cond}"
            _custom_sql_entries.append(entry)
            continue

        if check_type == 'null_check':
            cols = _validate_list(columns)
            for c in cols:  # emit one per column
                checks_yaml += (
                    f"  - missing_percent({c}):\n"
                    f"      name: null_check|{table_name}|{c}\n"
                    f"      warn: when > {config['warn_percentage']}%\n"
                    f"      fail: when > {config['fail_percentage']}%\n"
                )
            continue


        if check_type == 'reference':
            ref_table = check.get('reference_table')
            ref_col   = check.get('reference_column')
            cols = _validate_list(columns)
            for c in cols:
                checks_yaml += (
                    f"  - values in ({c}) must exist in {ref_table} ({ref_col}):\n"
                    f"      name: reference_check|{table_name}|{c}\n"
                )
            continue


        if check_type == 'duplicate':
            if (
                isinstance(columns, list)
                and columns
                and all(isinstance(g, list) and g and all(isinstance(c, str) for c in g) for g in columns)
            ):
                groups = columns
            else:
                groups = [_validate_list(columns)]
            for grp in groups:
                col_expr = ", ".join(grp)
                slug = "_".join(grp)
                checks_yaml += (
                    f"  - duplicate_count({col_expr}):\n"
                    f"      name: duplicate_check|{table_name}|{slug}\n"
                    f"      fail: when > 0\n"
                )
            continue


        if check_type == 'schema':
            cols = _validate_list(columns)
            required_columns_for_schema.extend(cols)
            continue

    if required_columns_for_schema:
        uniq_required = list(dict.fromkeys(required_columns_for_schema))
        checks_yaml += (
            f"  - schema:\n"
            f"      name: required_columns|{table_name}\n"
            f"      fail:\n"
            f"        when required column missing:\n"
        )
        for col in uniq_required:
            checks_yaml += f"          - {col}\n"
        checks_yaml += (
            f"  - schema:\n"
            f"      name: schema_changes|{table_name}\n"
            f"      fail:\n"
            f"        when schema changes:\n"
            f"          - column delete\n"
            f"          - column type change\n"
            f"          - column index change\n"
            f"      warn:\n"
            f"        when schema changes:\n"
            f"          - column add\n"
        )


    # Append any custom_sql entries outside the filter tag
    if _custom_sql_entries:
        checks_yaml += f"\nchecks for {dataset_name}:\n" + "\n".join(_custom_sql_entries) + "\n"
            

# print(checks_yaml)

print("=" * 75)
print("📋 DQ Scan Details:")
print("=" * 75) 

# ---------------------------------------
# ✅ Soda Cloud Config
# ---------------------------------------
def generate_soda_cloud_config(env):
    env_map = {
        "_prod": "prod", "_stg": "local", "_dev": "local",
        "_load_test": "local", "local": "local"
    }
    env_key = env_map.get(env, "local")

    print(f"🔐 Fetching SODA API credentials")

    api_key_id = dbutils.secrets.get(scope="soda-secrets", key="prod_api_key_id").strip()
    api_key_secret = dbutils.secrets.get(scope="soda-secrets", key="prod_api_key_secret").strip()

    if not api_key_id or not api_key_secret:
        raise RuntimeError("❌ Missing Soda Cloud API credentials for prod environment.")

    print("✅ Fetched SODA API credentials")
    if env_key == "local":
        print("ℹ️ Local mode detected — results will not be pushed to Soda Cloud.")       

    config_str = f"""
soda_cloud:
  host: cloud.us.soda.io
  api_key_id: "{api_key_id}"
  api_key_secret: "{api_key_secret}"
"""
    return config_str, env_key


# ---------------------------------------
# ✅ Print Run Info
# ---------------------------------------
def print_run_info(config_path, environment, date_config, date_filter_condition, execution_mode):
    mode = date_config.get('mode', 'today')
    from_date = date_config.get('from_date', '')
    to_date = date_config.get('to_date', '')
    date_column = date_config.get('date_column', 'dw_insert_ts')

    if mode == 'today':
        date_info = f"Using today's date: {date.today().isoformat()}"
    elif mode == 'single':
        date_info = f"Using single date: {from_date}"
    elif mode == 'range':
        date_info = f"Using date range: {from_date} to {to_date}"
    else:
        date_info = f"Using date filter mode: {mode}"

    print(f"✅ Using config: {config_path}")
    print(f"🌐 Environment: {environment}")
    print(f"⌨️ Date Config Mode: {mode}")
    print(f"📅 {date_info}")
    print(f"📋 Applied date filter: {date_filter_condition}")
    print(f"🧭 Execution mode: {execution_mode}")
    print("=" * 75)
    print(f"🔁 Starting DQ scan at: {datetime.now().isoformat()}")
    print("=" * 75)

# ---------------------------------------
# ✅ Get Data Source Name from Mapping with Fallback
# ---------------------------------------
def get_data_source_name(dataset_name: str, mapping: dict, default: str = "DNA_SODA_DATABRICKS_API_DATA_SOURCE") -> str:
    """
    Extracts source system and layer from dataset name and returns mapped Soda data source name.
    Falls back to default if no mapping found.
    Example: nero_dev.intermediate.fact_player_activity → source_system=nero, layer=intermediate
    """
    try:
        parts = dataset_name.split(".")
        source_env = parts[0]             # e.g., nero_dev
        layer = parts[1]                  # e.g., intermediate
        source_system = source_env.split("_")[0]  # e.g., nero

        data_source_name = mapping.get(source_system, {}).get(layer)
        if data_source_name:
            return data_source_name, source_system
        else:
            print(f"⚠️ No mapping found for source='{source_system}', layer='{layer}'. Using default: {default}")
            return default, source_system

    except Exception as e:
        print(f"⚠️ Error parsing data source from `{dataset_name}`: {e}. Using default: {default}")
        return default, source_system

# ---------------------------------------
# ✅ Execute Scan
# ---------------------------------------
def run_dq_checks():
    print("Importing SODA Scan...") 
    print_run_info(config_path, environment, config.get("date_config", {}), date_filter_condition, execution_mode)

    scan = Scan()

    soda_cloud_config, env_key = generate_soda_cloud_config(environment)
    
    print("🏁 SODA DQ Scan initiated") 

    is_local = env_key == "local" #is_local = True if env_key == "local" else False

    scan.add_configuration_yaml_str(soda_cloud_config)

    first_dataset_name = config.get("datasets", [])[0]["name"].format(env=environment)
    data_source_name, title_name = get_data_source_name(first_dataset_name, data_source_mapping)

    # soda scan formation
    scan_name=f"soda_scan_{title_name}_{os.path.basename(config_path).replace('.yml', '')}"
    scan.set_scan_definition_name(scan_name)
    print(f"🔍 Resolved Soda scan name: {scan_name}")

    scan.set_data_source_name(data_source_name)
    scan.add_spark_session(spark, data_source_name=data_source_name)

    print(f"🔍 Resolved Soda data source name: {data_source_name} (from dataset: {first_dataset_name})")
    print("=" * 75)
    
    scan.add_sodacl_yaml_str(checks_yaml)
    scan.set_is_local(is_local)

    scan.execute()

    logs = scan.get_logs_text()
    if "UNRESOLVED_COLUMN" in logs:
        raise RuntimeError(f"❌ Soda scan encountered unresolved column errors. Please fix the {config_path} config.")

    if "TABLE_OR_VIEW_NOT_FOUND" in logs:
        raise RuntimeError(f"❌ Soda scan encountered missing dataset/table error. Please check the {config_path} config and Spark tables.")

    print(scan.get_logs_text())
    scan.assert_no_checks_warn_or_fail()
    
    print("=" * 75)
    print(f"✅ DQ Scan Complete: {datetime.now().isoformat()}")
    print("=" * 75 + "\n")

# ---------------------------------------
# ✅ Main Execution Mode Handling
# ---------------------------------------
try:
    execution_mode = dbutils.widgets.get("soda_execution_mode")
except:
    dbutils.widgets.text("soda_execution_mode", "batch")
    execution_mode = dbutils.widgets.get("soda_execution_mode")

if execution_mode == "nrt":
    while True:
        run_dq_checks()
        time.sleep(1 * 60 * 60) # nrt dq checks execute every hour...
else:
    run_dq_checks()
