"""Спринт 1. Архитектура решения и решение проблем перегрузки.

Практическое задание: ETL-механизм загрузки фильмов.
Вариант с запросами к API elasticsearch.
"""

import json
import os
import sqlite3
from typing import Any, List

import requests


class ESLoader:
    def __init__(self, url: str):
        self.url = url

    def load_to_es(self, records: List[dict], index_name: str):
        """
        Метод для сохранения записей в Elasticsearch.
        :param records: список данных на запись, который должен быть следующего вида:
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
        :param index_name: название индекса, куда будут сохраняться данные
        """
        indices = [{
            'index': {
                '_index': 'movies',
                '_id': 'my_id{index}'.format(index=index if index > 1 else '')
            }} for index in range(1, len(records) + 1)
        ]
        zipped_records = zip(indices, records)
        preload_list = []
        for element in zipped_records:
            preload_list.extend(json.dumps(object) for object in list(element))
        data_to_load = '\n'.join(preload_list) + '\n'
        headers = {'Content-Type': 'application/x-ndjson'}
        request = requests.post(
            '{url}/_bulk'.format(url=self.url),
            headers=headers,
            data=data_to_load,
        )
        print(data_to_load)


class ETL:
    MOVIES_QUERY = """SELECT
            m.id,
            m.imdb_rating,
            m.genre,
            m.title,
            m.plot as description,
            m.director,
            m.writer,
            m.writers
        FROM movies m"""

    ACTORS_QUERY = """SELECT
            a.id,
            a.name
        FROM movie_actors ma
            LEFT JOIN actors a ON ma.actor_id = a.id
        WHERE ma.movie_id = ?"""

    WRITERS_QUERY = """SELECT
            id, name
        FROM writers
        WHERE id IN ({args})"""

    def __init__(self, conn: sqlite3.Connection, es_loader: ESLoader):
        self.es_loader = es_loader
        self.conn = conn

    def _transform_value(self, *, extracted_value) -> Any:
        """Transform value after extraction.

        Args:
            extracted_value: value, received after extraction

        Returns:
            Any
        """
        if extracted_value != ('N/A' or ''):
            return extracted_value

    def _get_actors(self, *, movie_id):
        """Get actors for the movie.

        Args:
            movie_id: ID of the specified movie

        Returns:
            List[Dict], List
        """
        actors = []
        actors_names = []
        for row in self.conn.cursor().execute(ETL.ACTORS_QUERY, (movie_id,)):
            actor_id = self._transform_value(extracted_value=row[0])
            actor_name = self._transform_value(extracted_value=row[1])
            actors.append(
                {
                    'id': actor_id,
                    'name': actor_name,
                },
            )
            actors_names.append(actor_name)

        if actors and actors_names:
            return actors, actors_names
        return None, None

    def _get_filter_on_writers(self, *, writer, writers):
        """Return list of writer IDs.

        Args:
            writer: ID of specified writer
            writers: JSON with IDs of writers

        Returns:
            List
        """
        filter_on_writers = []
        if writer:
            filter_on_writers.append(writer)
        elif writers:
            ls = json.loads(writers)
            filter_on_writers = [element['id'] for element in ls]
        return filter_on_writers

    def _get_writers(self, *, writer, writers):
        """Get writers for the movie.

        Args:
            writer: ID of specified writer
            writers: JSON with IDs of writers

        Returns:
            List, str
        """
        filter_on_writers = self._get_filter_on_writers(
            writer=writer,
            writers=writers,
        )
        writers = []
        writers_names = []
        for row in self.conn.cursor().execute(
            ETL.WRITERS_QUERY.format(args=','.join(['?']*len(filter_on_writers))),
            filter_on_writers,
        ):
            writer_id = self._transform_value(extracted_value=row[0])
            writer_name = self._transform_value(extracted_value=row[1])
            writers.append(
                {
                    'id': writer_id,
                    'name': writer_name,
                },
            )
            writers_names.append(writer_name)

        if writers and writers_names:
            return writers, writers_names
        return None, None

    def _transform_data(self, *, extracted_data):
        """Tranform extracted data.

        Args:
            extracted_data: raw data to be transformed

        Returns:
            Dict
        """
        movie_id = self._transform_value(extracted_value=extracted_data[0])
        actors, actors_names = self._get_actors(movie_id=movie_id)
        writers, writers_names = self._get_writers(
            writer=extracted_data[6],
            writers=extracted_data[7],
        )
        genre = self._transform_value(extracted_value=extracted_data[2])
        director = self._transform_value(extracted_value=extracted_data[5])

        return {
            'id': movie_id,
            'imdb_rating': (
                None if extracted_data[1] == ('N/A' or '') else float(extracted_data[1])
            ),
            'genre': genre.split(', ') if genre else None,
            'title': self._transform_value(extracted_value=extracted_data[3]),
            'description': self._transform_value(extracted_value=extracted_data[4]),
            'director': director.split(', ') if director else None,
            'actors': actors,
            'actors_names': actors_names,
            'writers': writers,
            'writers_names': writers_names,
        }

    def _extract_movies(self):
        """Extract dataset.

        Extract and return transformed dataset

        Returns:
            List
        """
        movie_cursor = self.conn.cursor()
        movies = []
        for row in movie_cursor.execute(ETL.MOVIES_QUERY):
            movies.append(self._transform_data(extracted_data=row))
        return movies

    def load(self, index_name: str):
        """
        Основной метод для нашего ETL.
        Обязательно используйте метод load_to_es, это будет проверяться
        :param index_name: название индекса, в который будут грузиться данные
        """
        movies = self._extract_movies()
        self.es_loader.load_to_es(movies, index_name)


def main():
    DB_FILE_NAME = 'db.sqlite'
    INDEX_NAME = 'movies'
    ELASTIC_HOST = 'http://0.0.0.0:9200'

    dirname = os.path.dirname(__file__)
    db = os.path.join(dirname, DB_FILE_NAME)
    connection = sqlite3.connect(db)

    es_loader = ESLoader(ELASTIC_HOST)
    etl = ETL(connection, es_loader)
    etl.load(INDEX_NAME)


if __name__ == '__main__':
    main()
