"""Спринт 1. Синхронные фреймворки.

Практическое задание: сервис на Flask
"""

from flask import Flask, request

from es import Elasticsearch
from schemas import MovieSchema

app = Flask(__name__)
es = Elasticsearch(url='http://0.0.0.0:9200', index='movies')


@app.route('/client/info')
def hello_world():
    """Return user agent info.

    Returns:
        dict
    """
    return {
        'user_agent': str(request.user_agent),
    }


@app.route('/api/movies')
def movie_list():
    if len(request.args) > 0:
        # Check if limit is integer
        limit = request.args.get('limit', 0)
        try:
            limit = int(limit)
        except ValueError:
            return 'ERROR: Limit should be an integer', 422

        # Check if page is integer
        page = request.args.get('page', 0)
        try:
            page = int(page)
        except ValueError:
            return 'ERROR: Page should be an integer', 422

        # Check if limit is positive
        if limit < 0:
            return 'ERROR: Limit should be positive or equals to zero', 422

        # Check if page is positive
        if page <= 0:
            return 'ERROR: Page should be positive', 422

        # Check if sort field is permitted
        sort = request.args.get('sort')
        if sort and sort not in {'id', 'title', 'imdb_rating'}:
            return 'ERROR: Sort field is not permitted', 422

        # Check if sort order is permitted
        sort = request.args.get('sort_order')
        if sort and sort not in {'asc', 'desc'}:
            return 'ERROR: Sort order is not permitted', 422

    movies = es.get_list(
        limit=request.args.get('limit', 50),
        page=request.args.get('page'),
        sort=request.args.get('sort'),
        sort_order=request.args.get('sort_order'),
        search=request.args.get('search', ''),
    )
    schema = MovieSchema()
    return schema.dumps(movies, many=True)


@app.route('/api/movies/')
def movie_detail_empty():
    return ''


@app.route('/api/movies/<string:movie_id>')
def movie_detail(movie_id):
    movie = es.get_detail(movie_id=movie_id)
    schema = MovieSchema()
    response = schema.dumps(movie)
    if movie:
        return response
    return '', 404
