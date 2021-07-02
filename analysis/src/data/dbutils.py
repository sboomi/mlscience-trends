import pymongo


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
