import dbt_project


def init(target: str):
    tests_env = Environment(target)
    tests_env.clear()
    tests_env.init()


class Environment:
    def __init__(self, target: str):
        self.dbt_runner = dbt_project.get_dbt_runner(target)

    def clear(self):
        self.dbt_runner.run_operation("elementary_tests.clear_env")

    def init(self):
        self.dbt_runner.run(selector="init")
        self.dbt_runner.run(select="elementary")
