import os
import requests

# Extracts the Titanic dataset
URL ="https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv",
LOCAL_TEMP_PATH = "/tmp/titanic.csv"
DBFS_PATH = "dbfs:/FileStore/skim_project11/titanic.csv"

def extract(url=URL, local_path=LOCAL_TEMP_PATH, dbfs_path=DBFS_PATH):
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    response = requests.get(url)
    response.raise_for_status()

    with open(local_path, "wb") as file:
        file.write(response.content)
    print(f"Data downloaded to local path: {local_path}")

    dbutils.fs.cp(f"file://{local_path}", dbfs_path)
    print(f"Data moved to DBFS path: {dbfs_path}")

    return dbfs_path

if __name__ == "__main__":
    extract()