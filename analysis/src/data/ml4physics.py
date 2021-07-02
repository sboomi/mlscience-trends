import logging
import re
import time
from pathlib import Path
from typing import List, Optional

import coloredlogs
import pandas as pd
import pymongo
import requests
from bs4 import BeautifulSoup
from pydantic import BaseModel, NoneStr

from .dbutils import load_mongo_client

ML4PHYSICS_URL = "https://ml4physicalsciences.github.io/"
CORE_API_URL = "https://core.ac.uk:443/api-v2/"

log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=log_fmt)
logger = logging.getLogger(__name__)
coloredlogs.install()


class DataMl4Physics(BaseModel):
    title: List[NoneStr] = []
    authors: List[NoneStr] = []
    year: List[Optional[int]] = []


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
                    data.year.append(y)
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
