import pytest
from main import main


def test_spark_completion(caplog):
    main()

    # Assert that the "Spark session completed!" message is in the logs
    assert "Spark session completed!" in caplog.text
