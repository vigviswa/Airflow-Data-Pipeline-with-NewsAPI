from dags import tempus_challenge_dag as t
from datetime import datetime, timedelta
import pytest


def test_fetch_data():
    # Arrange
    now = datetime.now().date()
    fromdate = now - timedelta(days=30)
    # Act
    response = t.fetch_data("everything", "techcrunch", fromdate)

    # Assert
    assert response == t.fetch_data("everything", "techcrunch", t.FETCH_FROM_DATE)
