import dbt_project


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
        self.dbt_runner.run(selector="init")
        self.dbt_runner.run(select="elementary")
