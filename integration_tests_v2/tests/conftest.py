import pytest
from dbt_project import DbtProject
from env import Environment


def pytest_addoption(parser):
    parser.addoption("--target", action="store", default="postgres")


def pytest_sessionstart(session):
    target = session.config.getoption("--target")
    tests_env = Environment(target)
    tests_env.clear()
    tests_env.init()


@pytest.fixture
def dbt_project(target: str) -> DbtProject:
    return DbtProject(target)


@pytest.fixture(scope="session")
def target(request) -> str:
    return request.config.getoption("--target")


@pytest.fixture
def test_id(request) -> str:
    return request.node.name
