"""Спринт 1. Архитектура решения и решение проблем перегрузки.

Практическое задание: ETL-механизм загрузки фильмов.
Вариант с библиотекой elasticsearch.
"""

import json
import os
import sqlite3
from typing import Any

import tqdm
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

DB_FILE_NAME = 'db.sqlite'
MAPPING_JSON_NAME = 'mapping.json'
INDEX_NAME = 'movies'
ELASTIC_HOST = '0.0.0.0:9200'
INDEX_EXISTS_CODE = 400

NA_PATTERN = 'N/A'

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

dirname = os.path.dirname(__file__)
db = os.path.join(dirname, DB_FILE_NAME)

connection = sqlite3.connect(db)
cursor = connection.cursor()


def transform_value(*, extracted_value) -> Any:
    """Transform value after extraction.

    Args:
        extracted_value: value, received after extraction

    Returns:
        Any
    """
    if extracted_value != 'N/A':
        return extracted_value


def stringify_list(*, ls):
    """Return a string of names.

    Args:
        ls: a list of dicts

    Returns:
        str
    """
    if ls:
        return ', '.join(
            [element['name'] for element in ls if element['name']]
        )


def get_actors(*, movie_id):
    """Get actors for the movie.

    Args:
        movie_id: ID of the specified movie

    Returns:
        List, str
    """
    actors = []
    for row in cursor.execute(ACTORS_QUERY, (movie_id,)):
        actors.append(
            {
                'id': transform_value(extracted_value=row[0]),
                'name': transform_value(extracted_value=row[1]),
            },
        )

    return (
        actors if actors else None,
        stringify_list(ls=actors),
    )


def get_filter_on_writers(*, writer, writers):
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


def get_writers(*, writer, writers):
    """Get writers for the movie.

    Args:
        writer: ID of specified writer
        writers: JSON with IDs of writers

    Returns:
        List, str
    """
    filter_on_writers = get_filter_on_writers(
        writer=writer,
        writers=writers,
    )
    writers = []
    for row in cursor.execute(
        WRITERS_QUERY.format(args=','.join(['?']*len(filter_on_writers))),
        filter_on_writers,
    ):
        writers.append(
            {
                'id': transform_value(extracted_value=row[0]),
                'name': transform_value(extracted_value=row[1]),
            },
        )

    return (
        writers if writers else None,
        stringify_list(ls=writers),
    )


def transform_data(*, extracted_data):
    """Tranform extracted data.

    Args:
        extracted_data: raw data to be transformed

    Returns:
        Dict
    """
    movie_id = transform_value(extracted_value=extracted_data[0])
    actors, actors_names = get_actors(movie_id=movie_id)
    writers, writers_names = get_writers(
        writer=extracted_data[6],
        writers=extracted_data[7],
    )

    return {
        'id': movie_id,
        'imdb_rating': (
            None if extracted_data[1] == 'N/A' else float(extracted_data[1]),
        ),
        'genre': transform_value(extracted_value=extracted_data[2]),
        'title': transform_value(extracted_value=extracted_data[3]),
        'description': transform_value(extracted_value=extracted_data[4]),
        'director': transform_value(extracted_value=extracted_data[5]),
        'actors': actors,
        'actors_names': actors_names,
        'writers': writers,
        'writers_names': writers_names,
    }


def extract_movies():
    """Extract dataset.

    Extract and return transformed dataset

    Returns:
        List
    """
    movie_cursor = connection.cursor()
    movies = []
    for row in movie_cursor.execute(MOVIES_QUERY):
        movies.append(transform_data(extracted_data=row))
    return movies


def create_index(*, client):
    """Create an index in Elasticsearch.

    Creates an index if one isn't already there.

    Args:
        client: Elasticsearch client
    """
    mapping = os.path.join(dirname, MAPPING_JSON_NAME)
    with open(mapping, 'r') as fcm:
        mapping = fcm.read()
    client.indices.create(
        index=INDEX_NAME,
        body=mapping,
        ignore=INDEX_EXISTS_CODE,
    )


def generate_actions(*, dataset):
    """Yield elements of dataset.

    Args:
        dataset: transformed dataset

    Yields:
        Dict
    """
    for index, element in enumerate(dataset):
        index += 1
        index = index if index > 1 else ''
        element['_id'] = 'my_id{index}'.format(index=index)
        yield element


def main():
    """Perform ETL."""
    print('Extracting and transforming dataset...')
    movies = extract_movies()
    number_of_movies = len(movies)

    client = Elasticsearch(hosts=[ELASTIC_HOST])

    print('Creating index...')
    create_index(client=client)

    print('Indexing dataset...')
    progress = tqdm.tqdm(unit='movies', total=number_of_movies)
    successes = 0
    for ok, _action in streaming_bulk(
        client=client, index=INDEX_NAME, actions=generate_actions(
            dataset=movies,
        ),
    ):
        progress.update(1)
        successes += ok

    print('Indexed {successes} of {total} movies.'.format(
        successes=successes,
        total=number_of_movies,
    ))


if __name__ == '__main__':
    main()
