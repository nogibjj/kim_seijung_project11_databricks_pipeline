import os
from main import extract  # load, query

# Constants for paths
DATASET_PATH = "../data/titanic.csv"
LOG_PATH = "log.md"


def test_extract():
    """Test the extract function."""
    # Clean up before running the test
    if os.path.exists(DATASET_PATH):
        os.remove(DATASET_PATH)

    # Execute extract function
    dataset_path = extract()

    # Check if the dataset was extracted successfully
    assert dataset_path == DATASET_PATH
    assert os.path.exists(DATASET_PATH)
