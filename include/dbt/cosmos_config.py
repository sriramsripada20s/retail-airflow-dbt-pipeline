from pathlib import Path
from cosmos.config import ProfileConfig, ProjectConfig

# Path to your dbt project directory
DBT_PROJECT_PATH = Path("/usr/local/airflow/include/dbt")

# Configure dbt profile settings for Astronomer Cosmos
DBT_CONFIG = ProfileConfig(
    profile_name="retail",
    target_name="dev",
    profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
)

# Configure dbt project location
DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)