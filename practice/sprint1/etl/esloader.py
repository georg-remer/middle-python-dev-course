"""Elasticsearch module."""

import json
import logging
import os
from typing import List
from urllib.parse import urljoin

import requests


class ESLoader(object):
    """Loading to Elasticsearch."""

    def __init__(self, url: str):
        self.url = url

    def create_index(self, *, index_name: str, mapping_file: str) -> None:
        """Create an index in Elasticsearch.

        Creates an index if one isn't already there.

        Args:
            index_name: Index name to be created
            mapping_file: Path to a file containing mapping
        """
        with open(mapping_file, 'r') as fcm:
            mapping_content = fcm.read()
        requests.put(self.url, data=mapping_content)

    def load_to_es(self, records: List[dict], index_name: str) -> None:
        """Load data to Elasticsearch.

        Метод для сохранения записей в Elasticsearch.

        Args:
            index_name: название индекса, куда будут сохраняться данные
            records: список данных на запись, который должен быть следующего вида:
                [
                    {
                        "id": "tt123456",
                        "genre": ["Action", "Horror"],
                        "writers": [
                            {
                                "id": "123456",
                                "name": "Great Divider"
                            },
                            ...
                        ],
                        "actors": [
                            {
                                "id": "123456",
                                "name": "Poor guy"
                            },
                            ...
                        ],
                        "actors_names": ["Poor guy", ...],
                        "writers_names": [ "Great Divider", ...],
                        "imdb_rating": 8.6,
                        "title": "A long time ago ...",
                        "director": ["Daniel Switch", "Carmen B."],
                        "description": "Long and boring description"
                    }
                ]
        Если значения нет или оно N/A, то нужно менять на None
        В списках значение N/A надо пропускать
        """
        indices = [{
            'index': {
                '_index': 'movies',
                '_id': 'my_id{index}'.format(index=index if index > 1 else '')
            }} for index in range(1, len(records) + 1)
        ]
        records_with_indices = zip(indices, records)

        json_object_list = []
        for element in records_with_indices:
            json_object_list.extend(json.dumps(object) for object in list(element))
        nbjson = '\n'.join(json_object_list) + '\n'

        headers = {'Content-Type': 'application/x-ndjson'}
        request = requests.post(
            urljoin(self.url, '_bulk'),
            headers=headers,
            data=nbjson,
        )
