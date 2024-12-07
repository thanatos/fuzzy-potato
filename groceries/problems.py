import json

from aiohttp import web


def problem(
    status_code: int,
    problem_type = None,
    detail = None,
) -> web.Response:
    data = {}
    if problem_type is not None:
        data['type'] = \
            f'https://groceries.roywellington.net/errors/{problem_type}'
    if detail is not None:
        data['detail'] = detail

    return web.Response(
        status=status_code,
        body=json.dumps(data).encode('utf-8'),
        headers={
            'Content-Type': 'application/problem+json',
        },
    )
