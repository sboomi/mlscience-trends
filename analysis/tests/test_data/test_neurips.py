import pytest
import os
import pymongo
import pandas as pd

from dotenv import load_dotenv, find_dotenv

from src.data import neurips


def test_load_mongo_client_from_environment_file():
    load_dotenv(find_dotenv())
    mongo_username = os.environ.get("MONGO_DB_USERNAME")
    mongo_password = os.environ.get("MONGO_DB_PASSWORD")
    client = neurips.load_mongo_client(mongo_username, mongo_password)
    dbs = client.list_database_names()
    client.close()
    assert type(dbs) is list


def test_load_mongo_client_invalid_credentials():
    mongo_username = ""
    mongo_password = ""
    with pytest.raises(pymongo.errors.InvalidURI):
        client = neurips.load_mongo_client(mongo_username, mongo_password)


def test_extract_ml4physics():
    df = neurips.extract_ml4physics()
    assert all(df.columns == ["title", "authors"])


def text_extract_ml4physics_with_save_file(tmp_path):
    df = neurips.extract_ml4physics(tmp_path / "ml4physics.csv")
    assert len(list(tmp_path.iterdir())) == 1
    assert list(tmp_path.iterdir())[0].name == "ml4physics.csv"
    open_df = pd.read_csv(tmp_path / "ml4physics.csv")
    assert all(open_df.columns == ["title", "authors"])


def test_post_ml4physics_info():
    load_dotenv(find_dotenv())
    mongo_username = os.environ.get("MONGO_DB_USERNAME")
    mongo_password = os.environ.get("MONGO_DB_PASSWORD")
    core_api_key = os.environ.get("CORE_API_KEY")
    neurips.post_ml4physics_info(
        mongo_username, mongo_password, core_api_key, mongo_database="test_neurips", mongo_collection="test_ml4physics",
    )
    client = neurips.load_mongo_client(mongo_username, mongo_password)
    df = neurips.extract_ml4physics()
    total_count = client["test_neurips"]["test_ml4physics"].count_documents({})
    client.close()
    assert total_count == df.shape[0]


def test_post_ml4physics_info_invalid_core_key():
    load_dotenv(find_dotenv())
    mongo_username = os.environ.get("MONGO_DB_USERNAME")
    mongo_password = os.environ.get("MONGO_DB_PASSWORD")
    core_api_key = "wrongapikey"
    with pytest.raises(Exception):
        neurips.post_ml4physics_info(
            mongo_username,
            mongo_password,
            core_api_key,
            mongo_database="test_neurips",
            mongo_collection="test_ml4physics",
        )


def test_get_neurips_hashs():
    df = neurips.get_neurips_hashs()
    assert all(df.columns == ["hash", "year"])


def text_get_neurips_hashs(tmp_path):
    df = neurips.get_neurips_hashs(tmp_path / "neurips_metadata.csv")
    assert len(list(tmp_path.iterdir())) == 1
    assert list(tmp_path.iterdir())[0].name == "neurips_metadata.csv"
    open_df = pd.read_csv(tmp_path / "neurips_metadata.csv")
    assert all(open_df.columns == ["hash", "year"])


def test_save_neurips_metadata():
    load_dotenv(find_dotenv())
    mongo_username = os.environ.get("MONGO_DB_USERNAME")
    mongo_password = os.environ.get("MONGO_DB_PASSWORD")
    neurips.save_neurips_metadata(
        mongo_username, mongo_password, mongo_database="test_neurips", mongo_collection="test_metadata",
    )
    client = neurips.load_mongo_client(mongo_username, mongo_password)
    total_count = client["test_neurips"]["test_metadata"].count_documents({})
    client.close()
    assert total_count > 0


def test_save_neurips_metadata_with_hash_csv(tmp_path):
    load_dotenv(find_dotenv())
    mongo_username = os.environ.get("MONGO_DB_USERNAME")
    mongo_password = os.environ.get("MONGO_DB_PASSWORD")
    hash_csv = tmp_path / "neurips_metadata.csv"

    neurips.save_neurips_metadata(
        mongo_username,
        mongo_password,
        hash_csv=hash_csv,
        mongo_database="test_neurips",
        mongo_collection="test_metadata",
    )
    client = neurips.load_mongo_client(mongo_username, mongo_password)
    total_count = client["test_neurips"]["test_metadata"].count_documents({})
    client.close()
    assert total_count > 0


def test_save_neurips_metadata_wrong_hash_csv(tmp_path):
    load_dotenv(find_dotenv())
    mongo_username = os.environ.get("MONGO_DB_USERNAME")
    mongo_password = os.environ.get("MONGO_DB_PASSWORD")
    hash_csv = tmp_path / "neurips_metadata.csv"
    with pytest.raises(FileNotFoundError):
        neurips.save_neurips_metadata(
            mongo_username,
            mongo_password,
            hash_csv=hash_csv,
            mongo_database="test_neurips",
            mongo_collection="test_metadata",
        )
