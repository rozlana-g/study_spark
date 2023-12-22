from dynaconf import Dynaconf
from pathlib import Path


THIS_DIR = Path(__file__).resolve(strict=True).parent
settings = Dynaconf(
    envvar_prefix="STUDY_SPARK",
    root_path=THIS_DIR,
    settings_files=["settings.toml", ".secrets.toml"],
    environments=True,
    load_dotenv=True,
    env_switcher="STUDY_SPARK_ENV",
)

settings.ROOT_DIR = Path(__file__).resolve(strict=True).parent.parent.parent
