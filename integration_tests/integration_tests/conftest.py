import pathlib

import pytest
from dbt.version import __version__
from packaging import version

from .dbt_project import DbtProject
from .utils import get_package_database_and_schema

DBT_PROJECT_DIR = pathlib.Path(__file__).parent.parent


def pytest_addoption(parser):
    parser.addoption(
        "--project-dir",
        action="store",
        default=str(DBT_PROJECT_DIR),
        help="dbt project dir to use",
    )
    parser.addoption(
        "--target", action="store", default="postgres", help="dbt project target to use"
    )


@pytest.fixture(scope="session")
def dbt_version():
    return version.parse(__version__)


@pytest.fixture(scope="session")
def dbt_target(request):
    return request.config.getoption("--target")


@pytest.fixture(scope="session")
def dbt_project_dir(request):
    return request.config.getoption("--project-dir")


@pytest.fixture(scope="session")
def dbt_project(dbt_project_dir, dbt_target):
    project = DbtProject(project_dir=dbt_project_dir, target=dbt_target)
    project.clear_test_env()
    yield project
    project.cleanup()


@pytest.fixture(autouse=True)
def elementary_schema(dbt_project: DbtProject):
    database, schema = get_package_database_and_schema(dbt_project)
    schema_relation = dbt_project.create_relation(
        database, schema, None
    ).without_identifier()
    dbt_project.execute_macro("dbt.create_schema", relation=schema_relation)
    return schema_relation
