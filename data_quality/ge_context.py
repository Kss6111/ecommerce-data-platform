from pathlib import Path
import great_expectations as gx

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def get_context():
    return gx.get_context(mode="file", project_root_dir=str(PROJECT_ROOT))
