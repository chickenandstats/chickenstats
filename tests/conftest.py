import os
from unittest.mock import patch

import pytest
import requests
from requests.adapters import HTTPAdapter

try:
    import pandas  # noqa: F401

    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:
    import pyarrow  # noqa: F401

    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

try:
    import matplotlib  # noqa: F401

    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

try:
    from PIL import Image  # noqa: F401

    HAS_PIL = True
except ImportError:
    HAS_PIL = False

requires_pandas = pytest.mark.skipif(not HAS_PANDAS, reason="pandas not installed")
requires_pyarrow = pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow not installed")
requires_matplotlib = pytest.mark.skipif(not HAS_MATPLOTLIB, reason="matplotlib not installed")
requires_pil = pytest.mark.skipif(not HAS_PIL, reason="pillow not installed")


# -----------------------------------------------------------------------------
# Network guard
# -----------------------------------------------------------------------------

MOCK_DATA_DIR = os.path.join(os.path.dirname(__file__), "tests_chicken_nhl", "mock_data")
FIXTURE_GAME_ID = 2023020001
REPORT_FILES = {"PL": "events.html", "RO": "rosters.html", "TH": "shifts_home.html", "TV": "shifts_away.html"}

# Tests with one of these markers are allowed to reach the network
NETWORK_MARKERS = ("nhl", "live")

_network = {"allowed": False}
_blocked_urls: list[str] = []
_real_send = HTTPAdapter.send
_real_get = requests.Session.get


def saved_response(url: str) -> requests.Response | None:
    """Return the saved NHL response for a game 2023020001 URL, or None for any other URL."""
    filename = None
    if f"gamecenter/{FIXTURE_GAME_ID}/play-by-play" in url:
        filename = "play_by_play.json"
    else:
        for report, report_file in REPORT_FILES.items():
            if f"/{report}{str(FIXTURE_GAME_ID)[4:]}.HTM" in url:
                filename = report_file

    if filename is None:
        return None

    response = requests.Response()
    response.url = url
    response.status_code = 200
    with open(os.path.join(MOCK_DATA_DIR, filename), "rb") as f:
        response._content = f.read()

    return response


def _guarded_get(self, url, *args, **kwargs):
    """Serve game 2023020001 from disk; pass every other request on."""
    if not _network["allowed"]:
        response = saved_response(str(url))
        if response is not None:
            return response

    return _real_get(self, url, *args, **kwargs)


def _guarded_send(self, request, *args, **kwargs):
    """Refuse a request that reaches the network, and remember it so the test fails."""
    if _network["allowed"]:
        return _real_send(self, request, *args, **kwargs)

    _blocked_urls.append(request.url)
    raise requests.exceptions.ConnectionError(f"network access is blocked in tests: {request.url}")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_protocol(item, nextitem):
    """Set whether the test about to run may reach the network, before its fixtures are built."""
    _network["allowed"] = any(item.get_closest_marker(marker) for marker in NETWORK_MARKERS)
    _blocked_urls.clear()
    yield


@pytest.fixture(scope="session", autouse=True)
def _network_guard():
    """Install the guard for the whole session."""
    with patch.object(requests.Session, "get", _guarded_get), patch.object(HTTPAdapter, "send", _guarded_send):
        yield


@pytest.fixture(autouse=True)
def _fail_on_network_access():
    """Fail a test that tried to reach the network without being marked for it."""
    yield
    if _blocked_urls:
        urls = sorted(set(_blocked_urls))
        pytest.fail(
            f"the test tried to reach the network ({len(urls)} URL(s), first: {urls[0]}). "
            "Use the saved game 2023020001, or mark the test with pytest.mark.nhl.",
            pytrace=False,
        )
