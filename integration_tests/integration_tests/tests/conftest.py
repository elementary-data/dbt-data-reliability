import env
import pytest
from dbt.version import __version__ as dbt_version
from dbt_project import DbtProject
from filelock import FileLock
from packaging import version


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


@pytest.fixture(autouse=True)
def skip_by_targets(request, target: str):
    if request.node.get_closest_marker("skip_targets"):
        skipped_targets = request.node.get_closest_marker("skip_targets").args[0]
        if target in skipped_targets:
            pytest.skip("Test unsupported for target: {}".format(target))


@pytest.fixture(autouse=True)
def only_on_targets(request, target: str):
    if request.node.get_closest_marker("only_on_targets"):
        requested_targets = request.node.get_closest_marker("only_on_targets").args[0]
        if target not in requested_targets:
            pytest.skip("Test unsupported for target: {}".format(target))


@pytest.fixture(autouse=True)
def requires_dbt_version(request):
    if request.node.get_closest_marker("requires_dbt_version"):
        required_version = request.node.get_closest_marker("requires_dbt_version").args[
            0
        ]
        if version.parse(dbt_version) < version.parse(required_version):
            pytest.skip(
                "Test requires dbt version {} or above, but {} is installed.".format(
                    required_version, dbt_version
                )
            )


@pytest.fixture
def dbt_project(target: str) -> DbtProject:
    return DbtProject(target)


@pytest.fixture(scope="session")
def target(request) -> str:
    return request.config.getoption("--target")


@pytest.fixture
def test_id(request) -> str:
    if request.cls:
        return f"{request.cls.__name__}_{request.node.name}"
    return request.node.name
