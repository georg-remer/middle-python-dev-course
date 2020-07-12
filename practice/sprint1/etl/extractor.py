"""Extraction and transformation module."""

import json
import sqlite3
from typing import Any, Dict, List

from esloader import ESLoader

NONE_PATTERNS = ('N/A', '')

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
    WHERE ma.movie_id = ?
    ORDER BY a.id"""

WRITERS_QUERY = """SELECT DISTINCT
        id, name
    FROM writers
    WHERE id IN ({args})"""


class ETL(object):
    """Extraction and transformation."""

    def __init__(self, conn: sqlite3.Connection, es_loader: ESLoader):
        """Construct object.

        Args:
            conn: Database connection
            es_loader: Elasticsearch instance
        """
        self.es_loader = es_loader
        self.conn = conn

    def _transform_value(self, *, raw_value: Any) -> Any:
        """Transform value after extraction.

        Args:
            raw_value: Raw value received during extraction

        Returns:
            Any
        """
        if raw_value not in NONE_PATTERNS:
            return raw_value

    def _get_actors(self, *, movie_id: str) -> (List[Dict], List[str]):
        """Get actors for the movie.

        Args:
            movie_id: ID of the specified movie

        Returns:
            List[Dict], List[str]
        """
        actors = []
        actors_names = []
        for row in self.conn.cursor().execute(ACTORS_QUERY, (movie_id,)):
            actor_id = self._transform_value(raw_value=row[0])
            actor_name = self._transform_value(raw_value=row[1])
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

    def _get_filter_on_writers(self, *, writer: str, writers: str) -> List[str]:
        """Return list of writer IDs.

        Args:
            writer: ID of specified writer
            writers: JSON with IDs of writers

        Returns:
            List[str]
        """
        filter_on_writers = []
        if writer:
            filter_on_writers.append(writer)
        elif writers:
            ls = json.loads(writers)
            filter_on_writers = [element['id'] for element in ls]
        return filter_on_writers

    def _get_writers(
        self,
        *,
        writer: str,
        writers: str,
    ) -> (List[Dict], List[str]):
        """Get writers for the movie.

        Args:
            writer: ID of specified writer
            writers: JSON with IDs of writers

        Returns:
            List[Dict], List[str]
        """
        filter_on_writers = self._get_filter_on_writers(
            writer=writer,
            writers=writers,
        )
        writers = []
        writers_names = []
        for row in self.conn.cursor().execute(
            WRITERS_QUERY.format(args=','.join(['?']*len(filter_on_writers))),
            filter_on_writers,
        ):
            writer_id = self._transform_value(raw_value=row[0])
            writer_name = self._transform_value(raw_value=row[1])
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

    def _transform_data(self, *, row: tuple) -> Dict:
        """Tranform extracted data.

        Args:
            row: raw data to be transformed

        Returns:
            Dict
        """
        movie_id = self._transform_value(raw_value=row[0])
        actors, actors_names = self._get_actors(movie_id=movie_id)
        writers, writers_names = self._get_writers(
            writer=row[6],
            writers=row[7],
        )
        genre = self._transform_value(raw_value=row[2])
        director = self._transform_value(raw_value=row[5])

        return {
            'id': movie_id,
            'genre': genre.split(', ') if genre else None,
            'writers': writers,
            'actors': actors,
            'writers_names': writers_names,
            'actors_names': actors_names,
            'imdb_rating': None if row[1] in NONE_PATTERNS else float(row[1]),
            'title': self._transform_value(raw_value=row[3]),
            'director': director.split(', ') if director else None,
            'description': self._transform_value(raw_value=row[4]),
        }

    def _extract_movies(self) -> List[Dict]:
        """Extract dataset.

        Extract and return transformed dataset

        Returns:
            List(Dict)
        """
        movie_cursor = self.conn.cursor()
        movies = []
        for row in movie_cursor.execute(MOVIES_QUERY):
            movies.append(self._transform_data(row=row))
        return movies

    def load(self, index_name: str) -> None:
        """Extract and trasnform data.

        Основной метод для нашего ETL.
        Обязательно используйте метод load_to_es, это будет проверяться

        Args:
            index_name: название индекса, в который будут грузиться данные
        """
        movies = self._extract_movies()
        self.es_loader.load_to_es(movies, index_name)
