import pytest
from src import clean as subject


# ==== mock objects ====

@pytest.fixture(scope="function")
def args_mock(mocker):
    mock = mocker.MagicMock()
    return mock


@pytest.fixture
def clean_targets_mock():
    return [
        'path/to/some/dir/',
        'foo/'
    ]
