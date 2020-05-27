""" Tests to run against the flask server. """
import pytest
import sys
import os
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "ceruleanserver"))
import anyserver  # pylint: disable=import-error


@pytest.fixture
def client():
    """ Each call returns a new instance of the app for tests to run against. """
    # To prevent the usual catching of internal app errors, set testing to true
    anyserver.app.testing = True
    yield anyserver.app.test_client()


def test_get(client):
    """Send a GET request and check the response"""

    rv = client.get("/")
    assert b"Unknown request type" in rv.data
