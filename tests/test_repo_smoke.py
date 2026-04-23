from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_expected_project_directories_exist():
    expected_dirs = [
        "airflow",
        "dbt",
        "docs",
        "scripts",
        "terraform",
        "tests",
    ]

    for name in expected_dirs:
        assert (REPO_ROOT / name).is_dir()


def test_expected_workflow_files_exist():
    workflow_dir = REPO_ROOT / ".github" / "workflows"
    expected_files = [
        "dbt_ci.yml",
        "docker_build.yml",
        "python_lint.yml",
        "terraform_apply.yml",
        "terraform_plan.yml",
    ]

    for name in expected_files:
        assert (workflow_dir / name).is_file()
