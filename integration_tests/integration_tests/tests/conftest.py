import env
import pytest
from dbt_project import DbtProject
from filelock import FileLock


def pytest_addoption(parser):
    parser.addoption("--target", action="store", default="postgres")


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "skip_targets(targets): skip test for the given targets"
    )


@pytest.fixture(scope="session", autouse=True)
def init_tests_env(target, tmp_path_factory, worker_id: str):
    # Tests are not multi-threaded.
    if worker_id == "master":
        env.init(target)
        return

    # Temp dir shared by all workers.
    tmp_dir = tmp_path_factory.getbasetemp().parent
    env_ready_indicator_path = tmp_dir / ".wait_env_ready"
    with FileLock(str(env_ready_indicator_path) + ".lock"):
        if env_ready_indicator_path.is_file():
            return
        else:
            env.init(target)
            env_ready_indicator_path.touch()


@pytest.fixture(autouse=True)
def skip_by_targets(request, target):
    if request.node.get_closest_marker("skip_targets"):
        skipped_targets = request.node.get_closest_marker("skip_targets").args
        if target in skipped_targets:
            pytest.skip("Test unsupported for target: {}".format(target))


@pytest.fixture
def dbt_project(target: str) -> DbtProject:
    return DbtProject(target)


@pytest.fixture(scope="session")
def target(request) -> str:
    return request.config.getoption("--target")


@pytest.fixture
def test_id(request) -> str:
    return request.node.name
