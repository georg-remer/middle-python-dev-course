"""Спринт 1. Синхронные фреймворки. Практическое задание: сервис на Flask."""

from flask import Flask, request

app = Flask(__name__)


@app.route('/client/info')
def hello_world():
    """Return user agent info.

    Returns:
        dict
    """
    return {
        'user_agent': str(request.user_agent),
    }
