import dbt_project


class Environment:
    def __init__(self, target: str):
        self.dbt_runner = dbt_project.get_dbt_runner(target)

    def clear(self):
        self.dbt_runner.run_operation("elementary_tests.clear_env")

    def init(self):
        self.dbt_runner.run_operation("elementary_tests.create_env")
        self.dbt_runner.run()
