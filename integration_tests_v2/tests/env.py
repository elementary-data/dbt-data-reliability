import dbt_project


class Environment:
    def __init__(self):
        self.dbt_runner = dbt_project.get_dbt_runner()

    def clear(self):
        self.dbt_runner.run_operation("clear_env")

    def init(self):
        self.dbt_runner.run()
