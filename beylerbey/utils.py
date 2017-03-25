from typing import Iterable


def join_str(items: Iterable, sep: str = ', ') -> str:
    return sep.join(map(str, items))
