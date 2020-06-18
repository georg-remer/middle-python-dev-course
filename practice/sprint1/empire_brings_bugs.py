"""Спринт 1. Отладка. Практическое задание: Империя приносит баги."""

import json
import os
from typing import List

dirname = os.path.dirname(__file__)
input_file = os.path.join(dirname, 'input.txt')
output_file = os.path.join(dirname, 'output.txt')


def unique_tags(payload: dict) -> List[str]:
    """Return a unique set of tags in string representation.

    Args:
        payload: JSON

    Returns:
        List(str)
    """
    tag_list = payload.get('tags', [])
    tag_dict = {str(element): element for element in tag_list}
    keys_unique = set(tag_dict.keys())
    tags_unique = []
    for key in keys_unique:
        tags_unique.append(tag_dict[key])
    return tags_unique


def main():
    """Read file and call necessary method."""
    payload = None
    with open(input_file, 'r') as fcm:
        payload = fcm.read()
    payload = json.loads(payload)
    print(unique_tags(payload))


if __name__ == '__main__':
    main()
