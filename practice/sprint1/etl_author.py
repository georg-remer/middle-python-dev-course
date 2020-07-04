import json
import logging
import sqlite3
from contextlib import contextmanager
from typing import List
from urllib.parse import urljoin

import requests

logger = logging.getLogger()

def dict_factory(cursor: sqlite3.Cursor, row: tuple) -> dict:
    '''
    Так как в sqlite нет встроенной фабрики для строк в виде dict, приходится делать самостоятельно
    '''
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

@contextmanager
def conn_context(db_path: str):
    '''
    Так как в sqlite нет контекстного менеджера для работы с соединениями,
    то добавляем его тут, чтобы грамотно закрывать соединения
    :param db_path: путь до базы данных
    '''
    conn = sqlite3.connect(db_path)
    conn.row_factory = dict_factory
    yield conn
    conn.close()

class ESLoader:
    def __init__(self, url: str):
        self.url = url

    def _get_es_bulk_query(self, rows: List[dict], index_name: str) -> List[str]:
        '''
        Подготавливает bulk-запрос в Elasticsearch
        '''
        prepared_query = []
        for row in rows:
            prepared_query.extend([
                json.dumps({'index': {'_index': index_name, '_id': row['id']}}),
                json.dumps(row)
            ])
        return prepared_query

    def load_to_es(self, records: List[dict], index_name: str):
        '''
        Отправка запроса в ES и разбор ошибок сохранения данных
        '''
        prepared_query = self._get_es_bulk_query(records, index_name)
        str_query = '\n'.join(prepared_query) + '\n'

        response = requests.post(
            urljoin(self.url, '_bulk'),
            data=str_query,
            headers={'Content-Type': 'application/x-ndjson'}
        )

        json_response = json.loads(response.content.decode())
        for item in json_response['items']:
            error_message = item['index'].get('error')
            if error_message:
                logger.error(error_message)

class ETL:
    SQL = '''
    /* Используем CTE для читаемости. Здесь нет прироста производительности (можно поменять на subquery) */
    WITH x as (
        -- Используем group_concat, чтобы собрать id и имена всех актеров в один список после join'а с таблицей actors
        -- Отметим, что порядок id и имен совпадает и по этому поводу не приходится беспокоиться
        -- Также стоит не забывать про many-to-many связь между таблицами фильмов и актеров
        SELECT m.id, group_concat(a.id) as actors_ids, group_concat(a.name) as actors_names
        FROM movies m
                 LEFT JOIN movie_actors ma on m.id = ma.movie_id
                 LEFT JOIN actors a on ma.actor_id = a.id
        GROUP BY m.id
    )
    -- Получаем список всех фильмов со сценаристами и актерами
    SELECT m.id, genre, director, title, plot, imdb_rating, x.actors_ids, x.actors_names,
           /* Этот CASE решает проблему в дизайне таблицы: если сценарист всего один, 
           то он в столбце writer и id записан просто строкой, иначе данные хранятся в столбце writers 
           и записаны в виде списка объектов JSON.
           Для исправления данной ситуации применяем хак: 
           приводим одиночные записи сценаристов к списку из одного объекта JSON и кладем все в поле writers */
           CASE
            WHEN m.writers = '' THEN '[{"id": "' || m.writer || '"}]'
            ELSE m.writers
           END AS writers
    FROM movies m
    LEFT JOIN x ON m.id = x.id
    '''

    def __init__(self, conn: sqlite3.Connection, es_loader: ESLoader):
        self.es_loader = es_loader
        self.conn = conn

    def load_writers_names(self) -> dict:
        '''
        Получаем список всех сценаристов, так как нет возможности их в одном запросе получить
        :return: словарь всех сценаристов вида
        {
            "Writer": {"id": "writer_id", "name": "Writer"},
            ...
        }
        '''
        writers = {}
        # Используем DISTINCT, чтобы отсекать возможные дубли
        for writer in self.conn.execute('''SELECT DISTINCT id, name FROM writers'''):
            writers[writer['id']] = writer
        return writers

    def _transform_row(self, row: dict, writers: dict) -> dict:
        '''
        Основная логика преобразования данных из sqlite во внутреннее представление,
        которое дальше будет уходить в Elasticsearch
        Решаемы проблемы:
        1) genre в БД указан в виде строки из 1 или нескольких жанров разделенных запятыми
            -> преобразовываем в список жанров
        2) writers из запроса в БД приходит в виде списка словарей id'шников -> обогащаем именами из полученных
            заранее сценаристов и добавляем к каждому id еще и имя
        3) actors формируем из двух полей запроса (actors_ids и actors_names) в список словарей,
            наподобие списка сценаристов
        4) для полей writers, imdb_rating, director и description меняем поля 'N/A' на None

        :param row: строка из БД
        :param writers: текущие сценаристы
        :return: строка пригодная для сохранения в Elasticsearch
        '''
        movie_writers = []
        writers_set = set()
        for writer in json.loads(row['writers']):
            writer_id = writer['id']
            if writers[writer_id]['name'] != 'N/A' and writer_id not in writers_set:
                movie_writers.append(writers[writer_id])
                writers_set.add(writer_id)

        actors = []
        actors_names = []
        if row['actors_ids'] is not None and row['actors_names'] is not None:
            actors = [
                {'id': _id, 'name': name}
                for _id, name in zip(row['actors_ids'].split(','), row['actors_names'].split(','))
                if name != 'N/A'
            ]
            actors_names = [x for x in row['actors_names'].split(',') if x != 'N/A']

        return {
            'id': row['id'],
            'genre': row['genre'].replace(' ', '').split(','),
            'writers': movie_writers,
            'actors': actors,
            'actors_names': actors_names,
            'writers_names': [x['name'] for x in movie_writers],
            'imdb_rating': float(row['imdb_rating']) if row['imdb_rating'] != 'N/A' else None,
            'title': row['title'],
            'director': row['director'].split(',') if row['director'] != 'N/A' else None,
            'description': row['plot'] if row['plot'] != 'N/A' else None
        }

    def load(self, index_name: str):
        '''
        Основной метод для нашего ETL.
        Обязательно используйте метод load_to_es, это будет проверяться
        :param index_name: название индекса, в который будут грузиться данные
        '''
        records = []

        writers = self.load_writers_names()

        for row in self.conn.execute(self.SQL):
            transformed_row = self._transform_row(row, writers)
            records.append(transformed_row)

        self.es_loader.load_to_es(records, index_name)