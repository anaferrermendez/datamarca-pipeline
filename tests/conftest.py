import pytest

from src.config import create_spark_session, load_config


@pytest.fixture(scope="session")
def config():
    return load_config()


@pytest.fixture(scope="session")
def spark(config):
    session = create_spark_session(config)
    yield session
    session.stop()
