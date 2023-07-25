import env
import pytest
from dbt_project import DbtProject
from filelock import FileLock


def pytest_addoption(parser):
    parser.addoption("--target", action="store", default="postgres")


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


@pytest.fixture
def dbt_project(target: str) -> DbtProject:
    return DbtProject(target)


@pytest.fixture(scope="session")
def target(request) -> str:
    return request.config.getoption("--target")


@pytest.fixture
def test_id(request) -> str:
    return request.node.name


@pytest.fixture(autouse=True)
def only_on_targets(target: str, request):
    if request.node.get_closest_marker('only_on_targets'):
        requested_targets = request.node.get_closest_marker('only_on_targets').args
        if target not in requested_targets:
            pytest.skip(f'Skipped on taret {target} as it is not in requested targets: {requested_targets}')


@pytest.fixture(autouse=True)
def skip_targets(target: str, request):
    if request.node.get_closest_marker('skip_targets'):
        skip_targets = request.node.get_closest_marker('skip_targets').args
        if target in skip_targets:
            pytest.skip(f'Skipped on taret {target}')
