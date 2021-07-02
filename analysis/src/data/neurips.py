import pymongo
import logging
import coloredlogs

import pandas as pd
import requests
from pathlib import Path
from pydantic import BaseModel, NoneStr
from bs4 import BeautifulSoup
from typing import List, Optional
from .dbutils import load_mongo_client


NEURIPS_URL = "https://papers.nips.cc/"


log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=log_fmt)
logger = logging.getLogger(__name__)
coloredlogs.install()


class HashYearDataFrame(BaseModel):
    hash: List[NoneStr] = []
    year: List[Optional[int]] = []


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
                logger.error(f"Entry {row.hash} already exists!")
            else:
                logger.info(f"Entry {row.hash} inserted!")
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


def download_neurips_papers(target_folder: Path, hash_csv: Optional[Path] = None, chunk_size: int = 512) -> None:
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
