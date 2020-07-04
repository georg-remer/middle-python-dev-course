"""Main module."""

import os
import sqlite3

from esloader import ESLoader
from extractor import ETL

DB_FILE_NAME = 'db.sqlite'
INDEX_NAME = 'movies'
ELASTIC_HOST = 'http://0.0.0.0:9200'
MAPPING_FILE = 'mapping.json'


def main():
    """Run main flow."""
    dirname = os.path.dirname(__file__)
    db = os.path.join(dirname, DB_FILE_NAME)
    connection = sqlite3.connect(db)

    mapping_file = os.path.join(dirname, MAPPING_FILE)
    es_loader = ESLoader(ELASTIC_HOST)
    es_loader.create_index(index_name=INDEX_NAME, mapping_file=mapping_file)

    etl = ETL(connection, es_loader)
    etl.load(INDEX_NAME)


if __name__ == '__main__':
    main()
