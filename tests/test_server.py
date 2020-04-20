""" Tests to run against the flask server. """
import pytest
import sys
import os
dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(f'{dir_path}/../ceruleanserver')
import ceruleanserver.anyserver as anyserver

@pytest.fixture
def client():
    """ Each call returns a new instance of the app for tests to run against. """
    # To prevent the usual catching of internal app errors, set testing to true
    anyserver.app.testing = True
    yield anyserver.app.test_client()

def test_get(client):
    """Send a GET request and check the response"""

    rv = client.get('/')
    assert b'Unknown request type' in rv.data