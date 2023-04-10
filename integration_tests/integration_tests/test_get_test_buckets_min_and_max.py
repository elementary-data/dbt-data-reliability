
from elementary.clients.dbt.dbt_runner import DbtRunner
from os.path import expanduser, join


def test_get_test_buckets_min_and_max(dbt_project_dir, dbt_target):
    dbt_runner = DbtRunner(
        project_dir=dbt_project_dir,
        profiles_dir=join(expanduser("~"), ".dbt"),
        target=dbt_target,
    )

    ret = dbt_runner.run_operation(
        "test_get_test_buckets_min_and_max"
    )
    #import pdb;pdb.set_trace()  # NO_COMMIT
    assert ret != 1
