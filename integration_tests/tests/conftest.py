import os
import shutil
from pathlib import Path
from tempfile import mkdtemp
from typing import Optional

import pytest
import yaml
from dbt.version import __version__ as dbt_version
from dbt_project import (
    DBT2_RUNNERS,
    PYTEST_XDIST_WORKER,
    SCHEMA_NAME_SUFFIX,
    DbtProject,
)
from elementary.clients.dbt.factory import RunnerMethod
from env import Environment
from logger import get_logger
from packaging import version

DBT_PROJECT_PATH = Path(__file__).parent.parent / "dbt_project"
DBT_FUSION_SUPPORTED_TARGETS = [
    "snowflake",
    "bigquery",
    "redshift",
    "databricks_catalog",
    "duckdb",
]

logger = get_logger(__name__)


def pytest_addoption(parser):
    parser.addoption("--target", action="store", default="postgres")
    parser.addoption("--skip-init", action="store_true", default=False)
    parser.addoption("--clear-on-end", action="store_true", default=False)
    parser.addoption("--runner-method", action="store", default=None)


@pytest.fixture(scope="session")
def project_dir_copy(runner_method: Optional[RunnerMethod]):
    dbt_project_copy_dir = mkdtemp(prefix="integration_tests_project_")
    try:
        shutil.copytree(
            DBT_PROJECT_PATH,
            dbt_project_copy_dir,
            dirs_exist_ok=True,
            symlinks=True,
        )
        _edit_packages_yml_to_include_absolute_elementary_package_path(
            dbt_project_copy_dir
        )
        _remove_python_models_for_dbt_fusion(dbt_project_copy_dir, runner_method)
        yield dbt_project_copy_dir
    finally:
        shutil.rmtree(dbt_project_copy_dir)


def _edit_packages_yml_to_include_absolute_elementary_package_path(
    project_dir_copy: str,
):
    logger.info(
        f"Editing packages.yml to include absolute elementary package path for project {project_dir_copy}"
    )

    packages_yml_path = Path(project_dir_copy) / "packages.yml"
    with packages_yml_path.open("r") as packages_yml_file:
        packages_yml = yaml.safe_load(packages_yml_file)

    packages_yml["packages"][0]["local"] = str(
        (DBT_PROJECT_PATH / packages_yml["packages"][0]["local"]).resolve()
    )
    with packages_yml_path.open("w") as packages_yml_file:
        yaml.dump(packages_yml, packages_yml_file)


def _remove_python_models_for_dbt_fusion(
    project_dir_copy: str, runner_method: Optional[RunnerMethod]
):
    if runner_method not in DBT2_RUNNERS:
        return

    logger.info(f"Removing python tests for project {project_dir_copy}")

    # walk on the models dir and delete python files
    for path in (Path(project_dir_copy) / "models").rglob("*.py"):
        path.unlink()


@pytest.fixture(scope="session", autouse=True)
def init_tests_env(
    target: str,
    skip_init: bool,
    clear_on_end: bool,
    project_dir_copy: str,
    runner_method: Optional[RunnerMethod],
):
    env = Environment(target, project_dir_copy, runner_method)
    if not skip_init:
        logger.info(
            "Initializing test environment (worker=%s, schema_suffix='%s')",
            PYTEST_XDIST_WORKER or "main",
            SCHEMA_NAME_SUFFIX,
        )
        env.clear()
        env.init()
        logger.info(
            "Initialization complete (worker=%s)",
            PYTEST_XDIST_WORKER or "main",
        )

    yield

    if clear_on_end:
        logger.info("Clearing tests environment")
        env.clear()
        logger.info("Clearing complete")


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
def skip_for_dbt_fusion(request, runner_method: Optional[RunnerMethod]):
    if request.node.get_closest_marker("skip_for_dbt_fusion"):
        if runner_method in DBT2_RUNNERS:
            pytest.skip("Test unsupported for dbt fusion")


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
def dbt_project(
    target: str, project_dir_copy: str, runner_method: Optional[RunnerMethod]
) -> DbtProject:
    return DbtProject(target, project_dir_copy, runner_method)


@pytest.fixture(scope="session")
def target(request) -> str:
    return request.config.getoption("--target")


@pytest.fixture(scope="session")
def skip_init(request) -> bool:
    return request.config.getoption("--skip-init")


@pytest.fixture(scope="session")
def clear_on_end(request) -> bool:
    return request.config.getoption("--clear-on-end")


def _profile_path(target: str) -> Optional[str]:
    """Return the ``path`` dbt resolves for *target*, if it can be resolved.

    Goes through dbt's own profile handling instead of reading ``profiles.yml``
    directly, so ``env_var`` and any other Jinja in ``path`` render the way dbt
    will render it, and an omitted ``path`` picks up dbt-duckdb's ``:memory:``
    default.  ``env_var`` needs the invocation context, hence the
    ``set_invocation_context`` call.

    Returns ``None`` when the profile cannot be resolved, so callers only act on
    a value they positively read.
    """
    from argparse import Namespace

    from dbt.config.runtime import RuntimeConfig
    from dbt.flags import set_from_args
    from dbt_common.context import set_invocation_context

    profiles_dir = os.environ.get("DBT_PROFILES_DIR", os.path.expanduser("~/.dbt"))
    args = Namespace(
        project_dir=str(DBT_PROJECT_PATH),
        profiles_dir=profiles_dir,
        target=target,
        threads=1,
        vars={},
        profile=None,
        PROFILES_DIR=profiles_dir,
        PROJECT_DIR=str(DBT_PROJECT_PATH),
    )
    try:
        set_invocation_context(os.environ)
        set_from_args(args, None)
        return getattr(RuntimeConfig.from_args(args).credentials, "path", None)
    except Exception:
        return None


@pytest.fixture(scope="session")
def runner_method(request, target: str) -> Optional[RunnerMethod]:
    runner_method_str = request.config.getoption("--runner-method")
    if runner_method_str:
        runner_method = RunnerMethod(runner_method_str)
        if runner_method in DBT2_RUNNERS and target not in DBT_FUSION_SUPPORTED_TARGETS:
            raise ValueError(f"Fusion runner is not supported for target: {target}")
        if (
            runner_method in DBT2_RUNNERS
            and target == "duckdb"
            and _profile_path(target) == ":memory:"
        ):
            raise ValueError(
                f"The {runner_method.value} runner runs dbt in a subprocess, which "
                "cannot share an in-memory DuckDB database with the tests. Set "
                "`path` to a file in the duckdb target of your profiles.yml."
            )
        return runner_method
    return None


@pytest.fixture
def test_id(request) -> str:
    if request.cls:
        return f"{request.cls.__name__}_{request.node.name}"
    return request.node.name
