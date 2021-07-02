import pytest
import os
from dotenv import load_dotenv, find_dotenv
from src.data import neurips


@pytest.fixture(scope="session", autouse=True)
def execute_before_any_test():
    load_dotenv(find_dotenv())
    mongo_username = os.environ.get("MONGO_DB_USERNAME")
    mongo_password = os.environ.get("MONGO_DB_PASSWORD")
    client = neurips.load_mongo_client(mongo_username, mongo_password)
    db = client["test_neurips"]
    for collection in db.list_collection_names():
        db[collection].drop()
    client.close()
