"""Elasticsearch adapter."""

import json
from typing import Any, Dict, Iterable

import requests


class Elasticsearch(object):

    def __init__(self, *, url: str, index: str) -> None:
        self.url = url
        self.index = index

    def _make_request(self, *, query: Dict) -> Dict:
        url = '{url}/{index}/_search'.format(
            url=self.url,
            index=self.index,
        )
        headers = {'Content-Type': 'application/x-ndjson'}
        response = requests.get(
            url,
            data=json.dumps(query),
            headers=headers,
        ).content
        return json.loads(response)

    def get_detail(self, *, movie_id: str) -> Dict:
        """Get movie detail.

        Args:
            movie_id: Movie ID

        Returns:
            Dict
        """
        query = {
            'query': {
                'match': {
                    'id': movie_id,
                },
            },
        }
        response = self._make_request(query=query)
        source = response['hits']['hits']
        if source:
            detail = source[0]['_source']
            return {
                'id': detail.get('id'),
                'title': detail.get('title'),
                'description': detail.get('description'),
                'imdb_rating': detail.get('imdb_rating'),
                'writers': detail.get('writers'),
                'actors': detail.get('actors'),
                'genre': detail.get('genre'),
                'director': detail.get('director'),
            }
        return None

    def get_list(
        self,
        *,
        limit: int,
        page: int,
        sort: str,
        sort_order: str,
        search: str,
    ) -> Iterable[Any]:
        query = {}
        if limit:
            query.update({'size': limit})
            from pprint import pprint; pprint(query)
        if page:
            query.update({'from': (int(page) - 1)})
        if sort and sort_order:
            query.update(
                {
                    'sort': [{sort: sort_order}],
                },
            )
        else:
            query.update(
                {
                    'sort': [{'id': 'asc'}],
                },
            )
        if search:
            query.update(
                {
                    'query': {
                        'multi_match': {
                            'query': search,
                            # 'fuzziness': 'auto',
                            'fields': [
                                'title^10',
                                'description^4',
                                'genre^3',
                                'actors_names^3',
                                'writers_names^2',
                                'director',
                            ],
                        },
                    },
                },
            )
        else:
            query.update(
                {
                    'query': {
                        'match_all': {},
                    },
                },
            )
        response = self._make_request(query=query)
        source = response['hits']['hits']
        ls = []
        if source:
            for entry in source:
                ls.append(entry['_source'])
        return ls
