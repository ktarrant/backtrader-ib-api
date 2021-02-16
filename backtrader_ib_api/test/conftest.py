import pytest

from backtrader_ib_api.request_wrapper import RequestWrapper
from ibapi.client import EClient


def pytest_addoption(parser):
    parser.addoption("--host", default="127.0.0.1", help="Trader Workstation API Hostname")
    parser.addoption("--port", default=7496, type=int, help="Trader Workstation API Port")
    parser.addoption("--clientid", default=0, type=int, help="Trader Workstation API Client ID")


@pytest.fixture(scope="session")
def wrapper(request):
    wrapper = RequestWrapper()
    wrapper.start_app(request.config.getoption("--host"),
                      request.config.getoption("--port"),
                      request.config.getoption("--clientid"))

    def finalizer():
        wrapper.stop_app()
    request.addfinalizer(finalizer)

    return wrapper
