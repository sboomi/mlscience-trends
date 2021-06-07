import os
import re
import pymongo
import time
import logging
import coloredlogs

import pandas as pd
import requests
from pathlib import Path
from pydantic import BaseModel, NoneStr
from bs4 import BeautifulSoup
from typing import List, Tuple, Optional, Union

ML4PHYSICS_URL = "https://ml4physicalsciences.github.io/"
NEURIPS_URL = "https://papers.nips.cc/"
CORE_API_URL = "https://core.ac.uk:443/api-v2/"

log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=log_fmt)
logger = logging.getLogger(__name__)
coloredlogs.install()


class DataMl4Physics(BaseModel):
    title: List[NoneStr] = []
    authors: List[NoneStr] = []


class HashYearDataFrame(BaseModel):
    hash: List[NoneStr] = []
    year: List[Optional[int]] = []


def load_mongo_client(mongo_username: str, mongo_password: str) -> pymongo.MongoClient:
    """Loads MongoDB's client from Mongo credentials

    Parameters
    ----------
    mongo_username : str
        MongoDB's database username
    mongo_password : str
        MongoDB's database password

    Returns
    -------
    pymongo.MongoClient
        Database client of MongoDB
    """
    mongo_uri = f"mongodb+srv://{mongo_username}:{mongo_password}@maincluster.otbuf.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
    client = pymongo.MongoClient(mongo_uri)
    return client


def extract_ml4physics(save_file: Optional[Path] = None) -> pd.DataFrame:
    """Takes the information on the official website of `https://ml4physicalsciences.github.io/` and
    produces a dataframe with the title and the authors as columns

    Returns
    -------
    pd.DataFrame
        A dataframe containing the title and the authors of each entry
    """
    data = DataMl4Physics()
    for y in range(2017, 2021):
        url = f"{ML4PHYSICS_URL}{y}"
        r = requests.get(url)
        if r.status_code == 200:
            soup = BeautifulSoup(r.content, "html.parser")
            html_table = soup.select_one("section#papers").select_one("div.table-wrapper")
            for el in html_table.select("td"):
                if not el.text.isdigit():
                    title, *_, authors = re.split(r"\[(pdf|poster|video)\]", el.text.strip())
                    data.title.append(title.strip())
                    data.authors.append(authors.strip())
    df = pd.DataFrame(data.dict())
    if save_file:
        df.to_csv(save_file, index=None)
    return df


def post_ml4physics_info(
    mongo_username: str,
    mongo_password: str,
    core_api_key: str,
    mongo_database: str = "neurips",
    mongo_collection: str = "ml4physics",
) -> None:
    """Takes information from the ML4Physics table and transfers it to a MongoDB cluster.
    The extra information will be read thanks to CORE's API for extracting information on
    science papers. More information can be found here: `https://core.ac.uk/`.

    Parameters
    ----------
    mongo_username : str
        [description]
    mongo_password : str
        [description]
    core_api_key : str
        [description]
    mongo_database : str, optional
        [description], by default "neurips"
    mongo_collection : str, optional
        [description], by default "ml4physics"

    Raises
    ------
    Exception
        [description]
    """
    client = load_mongo_client(mongo_username, mongo_password)
    q_params = {"apiKey": core_api_key}
    db = client[mongo_database]
    collection = db[mongo_collection]
    df = extract_ml4physics()
    for index, row in df.iterrows():
        # Fix timer at 2 to avoid code 429
        time.sleep(3)
        query = row.title + " " + row.authors
        r = requests.get(f"{CORE_API_URL}search/{query}", params=q_params)
        if r.status_code == 200:
            entry = r.json()["data"][0]
            try:
                entry_id = collection.insert_one(entry).inserted_id
            except pymongo.errors.DuplicateKeyError:
                logger.error(f"Entry {entry['_id']} already exists")
            else:
                logger.info(f"{entry_id} inserted!")
        elif r.status_code == 400:
            logger.error(f"{r.status_code}: parameter invalid")
        elif r.status_code == 401:
            logger.error(f"{r.status_code}: API key invalid")
            raise Exception("Parameter `api_key` invalid")
        elif r.status_code == 429:
            logger.error(f"{r.status_code}: too many requests. Waiting 5s...")
            time.sleep(5)
    logger.info("Operation complete")


def get_neurips_hashs(save_file: Optional[Path] = None) -> pd.DataFrame:
    """Retrieves the hashes from each NeurIPS abstract from the proceedings website: `https://papers.nips.cc/`

    The hashes and the year are needed to extract complementary information from that website, hence why the user can pass an optional parameter where the resut is stored in a CSV file.

    Parameters
    ----------
    save_file : Optional[Path], optional
        The file where the results are saved, by default None

    Returns
    -------
    pd.DataFrame
        The dataframe containing the hashs and year columns
    """
    year_hash = HashYearDataFrame()
    for y in range(1987, 2021):
        year_url = NEURIPS_URL + f"paper/{y}"
        r = requests.get(year_url)
        if r.status_code == 200:
            soup = BeautifulSoup(r.content, "html.parser")
            for p in soup.select("div.col li a"):
                url = p["href"]
                *_, abstract = url.split("/")
                hash_url, *_ = abstract.split("-")
                year_hash.hash.append(hash_url)
                year_hash.year.append(y)
    df = pd.DataFrame(year_hash.dict())
    if save_file:
        df.to_csv(save_file, index=None)
    return df


def save_neurips_metadata(
    mongo_username: str,
    mongo_password: str,
    hash_csv: Optional[Path] = None,
    mongo_database: str = "neurips",
    mongo_collection: str = "neurips_metadata",
) -> None:
    """[summary]

    Parameters
    ----------
    mongo_username : str
        [description]
    mongo_password : str
        [description]
    hash_csv : Optional[Path], optional
        [description], by default None
    mongo_database : str, optional
        [description], by default "neurips"
    mongo_collection : str, optional
        [description], by default "neurips_metadata"
    """
    if hash_csv:
        try:
            year_hash = pd.read_csv(hash_csv)
        except FileNotFoundError:
            logger.error(f"{hash_csv} not found. Retrieving data")
            year_hash = get_neurips_hashs(save_file=hash_csv)
    else:
        year_hash = get_neurips_hashs()
    client = load_mongo_client(mongo_username, mongo_password)
    db = client[mongo_database]
    collection = db[mongo_collection]

    for i, row in year_hash.iterrows():
        json_url = f"https://papers.nips.cc/paper/{row.year}/file/{row.hash}-Metadata.json"
        req_json = requests.get(json_url)
        if req_json.status_code == 200:
            metadata = req_json.json()
            metadata["_id"] = row.hash
            try:
                metadata_id = collection.insert_one(metadata).inserted_id
            except pymongo.errors.DuplicateKeyError:
                logger.error(f"Entry {row.hash_url} already exists!")
            else:
                logger.info(f"Entry {row.hash_url} inserted!")
        else:
            logger.error(f"Error: code {req_json.status_code} for ID {row.hash}. Continuing...")
    logger.info("Operation complete.")


def download_neurips_bibtex(target_folder: Path, hash_csv: Optional[Path] = None) -> None:
    """[summary]

    Parameters
    ----------
    target_folder : Path
        [description]
    hash_csv : Optional[Path], optional
        [description], by default None
    """
    if not target_folder.exists():
        logger.warning("Provided folder doesn't exist. Creating a new one")
        target_folder.mkdir(parents=True)

    if hash_csv:
        try:
            year_hash = pd.read_csv(hash_csv)
        except FileNotFoundError:
            logger.error(f"{hash_csv} not found. Retrieving data")
            year_hash = get_neurips_hashs(save_file=hash_csv)
    else:
        year_hash = get_neurips_hashs()
    for i, row in year_hash.iterrows():
        bibtex_url = f"https://papers.nips.cc/paper/{row.year}/file/{row.hash}-Bibtex.bib"
        r = requests.get(bibtex_url)
        if r.status_code == 200 and not (target_folder / f"{row.year}_{row.hash}.bib").exists():
            with open(target_folder / f"{row.year}_{row.hash}.bib", "wb") as f:
                f.write(r.content)
            logger.info(f"{row.year}_{row.hash}.bib successfully written.")
    logger.info("Operation complete")


def download_neurips_papers(
    target_folder: Path, hash_csv: Optional[Path] = None, chunk_size: int = 512
) -> None:
    """[summary]

    Parameters
    ----------
    target_folder : Path
        [description]
    hash_csv : Optional[Path], optional
        [description], by default None
    chunk_size : int, optional
        [description], by default 512
    """
    if not target_folder.exists():
        logger.warning("Provided folder doesn't exist. Creating a new one")
        target_folder.mkdir(parents=True)

    if hash_csv:
        try:
            year_hash = pd.read_csv(hash_csv)
        except FileNotFoundError:
            logger.error(f"{hash_csv} not found. Retrieving data")
            year_hash = get_neurips_hashs(save_file=hash_csv)
    else:
        year_hash = get_neurips_hashs()
    for i, row in year_hash.iterrows():
        bibtex_url = f"https://papers.nips.cc/paper/{row.year}/file/{row.hash}-Paper.pdf"
        r = requests.get(bibtex_url, stream=True)
        if r.status_code == 200 and not (target_folder / f"{row.year}_{row.hash}.pdf").exists():
            with open(target_folder / f"{row.year}_{row.hash}.pdf", "wb") as f:
                for chunk in r.iter_content(chunk_size):
                    f.write(chunk)
            logger.info(f"{row.year}_{row.hash}.pdf successfully written.")
    logger.info("Operation complete")
