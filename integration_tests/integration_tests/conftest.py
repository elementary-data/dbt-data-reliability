import pathlib

import pytest
from dbt_osmosis.core.osmosis import DbtProject

DBT_PROJECT_DIR = pathlib.Path(__file__).parent.parent


def pytest_addoption(parser):
    parser.addoption(
        "--project-dir", action="store", default=str(DBT_PROJECT_DIR), help="dbt project dir to use"
    )
    parser.addoption(
        "--target", action="store", default="postgres", help="dbt project target to use"
    )


@pytest.fixture
def dbt_target(request):
    return request.config.getoption("--target")


@pytest.fixture
def dbt_project_dir(request):
    return request.config.getoption("--project-dir")


@pytest.fixture
def dbt_project(dbt_project_dir, dbt_target):
    return DbtProject(
        project_dir=dbt_project_dir,
        target=dbt_target
    )
