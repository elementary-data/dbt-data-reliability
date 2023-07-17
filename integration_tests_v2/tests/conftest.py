import pytest
from dbt_project import get_dbt_runner
from elementary.clients.dbt.dbt_runner import DbtRunner
from env import Environment


def pytest_addoption(parser):
    parser.addoption("--target", action="store", default="postgres")


def pytest_sessionstart(session):
    target = session.config.getoption("--target")
    tests_env = Environment(target)
    tests_env.clear()
    tests_env.init()


@pytest.fixture
def dbt_runner(target: str) -> DbtRunner:
    return get_dbt_runner(target)


@pytest.fixture(scope="session")
def target(request) -> str:
    return request.config.getoption("--target")
