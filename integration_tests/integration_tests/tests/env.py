import dbt_project


class Environment:
    def __init__(self, target: str):
        self.dbt_runner = dbt_project.get_dbt_runner(target)

    def clear(self):
        self.dbt_runner.run_operation("clear_env")

    def init(self):
        self.dbt_runner.run()
