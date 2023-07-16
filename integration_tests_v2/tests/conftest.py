from env import Environment


def pytest_sessionstart(session):
    tests_env = Environment()
    tests_env.clear()
    tests_env.init()
