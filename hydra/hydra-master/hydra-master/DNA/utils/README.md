# Databricks Asset Bundle - Storage Cost Calculator

## Overview

This Databricks asset bundle is designed to compute and analyze storage costs for tables in various schemas and business units. It calculates the storage cost based on the size of the tables and a predefined cost per GB. The results are stored in a target table in a specified catalog and schema.

## Bundle Components

1. **Databricks Asset Bundle Definition (`databricks.yml`)**: 
   - Defines the bundle, including artifacts, variables, and targets for development staging, and production environments.

2. **Storage Cost Computation Script (`compute_storage_cost.py`)**:
   - A Python script utilizing PySpark to compute storage costs. It requires specific parameters such as environment, business units, schemas, and cost per GB.

3. **Job Configuration (`storage_costs_config.yml`)**:
   - Defines a Databricks job to run the `compute_storage_cost.py` script. Configures job clusters, email notifications, and task parameters.

## Getting Started

### Configuration

1. **Databricks Asset Bundle (`databricks.yml`)**:
   - Customize the variables as needed:
     - `environment`: The environment for the job (e.g., `_dev`, `_stg`, or empty for production).
     - `BUs`: Business Units to include in the computation. Example: horizon, gamesec, core, common
     - `schemas`: Schemas to include in the computation.Example: raw, managed, intermediate.
     - `target_catalog`: Catalog where the cost data will be stored.
     - `target_schema`: Schema where the cost data will be stored.
     - `target_table`: Table where the results will be stored.
     - `cost_per_gb`: Cost per GB for storage.

2. **Storage Cost Computation Script (`compute_storage_cost.py`)**:
   - Ensure all dependencies are included in the wheel package.
   - Update the script if needed to match the specific requirements of your environment.

3. **Job Configuration (`storage_cost_config.yml`)**:
   - Adjust the job cluster settings according to your resource needs.
   - Ensure that the job cluster is configured with appropriate instance types and configurations.
   - Set up email notifications for job failures.

### Deployment

1. **Upload Assets**:
   - Upload the `databricks.yml`, `compute_storage_cost.py`, and `storage_cost_config.yml` files to your Databricks workspace.

2. **Deploy the Bundle**:
   - Use the Databricks CLI or Databricks UI to deploy the asset bundle to the desired environment.

3. **Run the Job**:
   - Execute the job configured in `storage_cost_config.yml` to start the storage cost computation process.