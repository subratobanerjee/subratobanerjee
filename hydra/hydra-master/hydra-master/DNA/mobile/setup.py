from setuptools import setup, find_packages

PACKAGE_NAME = "mobile"
AUTHOR = "2k_DE"
DESCRIPTION = "Package to encapsulate src folder"

setup(
    name=PACKAGE_NAME,
    version="0.6",
    packages=find_packages(where='master_id/src'),
    package_dir={'': 'master_id/src'},
    entry_points={
        "mobile": [
            "master_id=batch.managed.nba2km_mobile_master_id:run_batch",
            "master_id=batch.managed.wwesc_mobile_master_id:run_batch"]
    },
    url="",
    author=AUTHOR,
    description=AUTHOR,
    install_requires=['mobile', 'wheel', 'setuptools']
)
