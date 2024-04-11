import os

import dbt_project

PYTEST_XDIST_WORKER = os.environ.get("PYTEST_XDIST_WORKER", "")


def init(target: str, project_dir: str):
    tests_env = Environment(target, project_dir)
    tests_env.clear()
    tests_env.init()


class Environment:
    def __init__(self, target: str, project_dir: str):
        self.dbt_runner = dbt_project.get_dbt_runner(target, project_dir)

    def clear(self):
        self.dbt_runner.run_operation("elementary_tests.clear_env")

    def init(self):
        self.dbt_runner.run(selector="init", capture_output=True)
        command_args = ["run"]
        command_args.extend(["-s", "elementary"])
        s, output = self.dbt_runner._run_command(
            command_args=command_args,
            vars=None,
            quiet=False,
            capture_output=True,
        )
        for log in output:
            open(f"/tmp/dbt_worker_{PYTEST_XDIST_WORKER}.log", "a").write(log)
