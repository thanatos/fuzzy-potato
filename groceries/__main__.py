import argparse
import asyncio
import base64
import datetime
import hashlib
import json
import logging
from pathlib import Path
import random
import socket
import tomllib

from aiohttp import web
from uniseg import graphemecluster

from . import postgres_driver
from . import problems


HTML = Path(__file__).parent / 'html'
FOOD_EMOJI = '''
☕️🌭🌮🌯🌰
🌶️
🌽🌾
🍄🍅🍆🍇🍈🍉🍊🍋🍌🍍🍎🍏
🍐🍑🍒🍓🍔🍕🍖🍗🍘🍙🍚🍛🍜🍝🍞🍟
🍠🍡🍢🍣🍤🍥🍦🍧🍨🍩🍪🍫🍬🍭🍮🍯
🍰🍱🍲🍳
🥐🥑🥒🥓🥔🥕🥖🥗🥘🥙🥚🥛🥜🥝🥞🥟
🥠🥡🥢🥣🥤🥥🥦🥧🥨🥩🥪🥫🥬🥭🥮🥯
🧀🧁🧂🧃🧄🧅🧆🧇🧈🧉
🫐🫑🫒🫓🫔🫘🫙🫚🫛
'''
FOOD_EMOJI = FOOD_EMOJI.replace('\n', '')
FOOD_EMOJI = list(graphemecluster.grapheme_clusters(FOOD_EMOJI))


def error_handling(request_handler):
    async def error_handling_wrapper(request):
        try:
            return await request_handler(request)
        except NeedQVar as err:
            qvar = err.missing_var
            return problems.problem(
                400,
                problem_type = None,
                detail=f'Need query string parameter {qvar!r}'
            )

    return error_handling_wrapper


def emoji(html: str) -> str:
    emoji = ''.join(random.sample(FOOD_EMOJI, 3))
    html = html.replace('{{ GROCERY_EMOJI }}', emoji)
    return html


async def read_text_file(path):
    def do_read():
        return path.read_text(encoding='utf-8')

    return await asyncio.get_running_loop().run_in_executor(None, do_read)


async def fe_index(request):
    p = await read_text_file(HTML / 'index.html')
    p = emoji(p)
    return html_response(p)


async def fe_list(request):
    p = await read_text_file(HTML / 'list.html')
    p = emoji(p)
    return html_response(p)


async def get_list_collection(request):
    driver = request.app['db_driver']
    current = request.query.get('current')
    if current == '':
        list_id = await driver.current_list()
        return json_response(f'/api/list/{list_id}')
    else:
        lists = await driver.get_lists()
        if request.path.endswith('/'):
            return json_response(lists)
        else:
            return json_response([f'list/{i}' for i in lists])


@error_handling
async def post_list_collection(request):
    driver = request.app['db_driver']
    action = need_qvar(request, 'action')
    if action == 'new':
        now = datetime.datetime.utcnow()
        new_list_id = await driver.new_list(now)
        return json_response(f'/api/list/{new_list_id}')
    else:
        return bad_request()


async def get_list(request):
    list_id = request.match_info['list_id']
    logging.info('Request for list %s', list_id)
    driver = request.app['db_driver']
    try:
        sequence, grocery_list = await driver.get_list(list_id)
    except postgres_driver.NoSuchList:
        return no_such_list()

    item_json = []
    for item in grocery_list.items:
        item_json.append({
            'item_name': item.name,
            'item_index': item.index,
            'in_cart': item.in_cart,
            'purchase_price': item.purchase_price,
        })
    json = {
        'created_at': grocery_list.created_at.isoformat(),
        'items': item_json,
    }

    logging.info('%r', grocery_list)
    extra_headers = {
        'ETag': f'W/"sequence-{sequence}"',
    }
    return json_response(json, extra_headers=extra_headers)


async def post_list(request):
    list_id = request.match_info['list_id']
    driver = request.app['db_driver']
    action = request.query.get('action')
    if action == 'add-item':
        body = await request.json()
        item_name = body['item_name']
        await driver.add_item(list_id, item_name)
        return no_content()
    elif action == 'remove-item':
        body = await request.json()
        item_name = body['item_name']
        item_index = body['item_index']
        logging.info(
            'Removing from list: (%r, %r, %r)',
            list_id, item_name, item_index,
        )
        await driver.remove_item(list_id, item_name, item_index)
        return no_content()
    elif action == 'add-to-cart':
        body = await request.json()
        item_name = body['item_name']
        item_index = body['item_index']
        logging.info(
            'Adding to cart: (%r, %r, %r)',
            list_id, item_name, item_index,
        )
        await driver.mark_item_as_gotten(list_id, item_name, item_index)
        return no_content()
    elif action == 'remove-from-cart':
        body = await request.json()
        item_name = body['item_name']
        item_index = body['item_index']
        logging.info(
            'Removing from cart: (%r, %r, %r)',
            list_id, item_name, item_index,
        )
        await driver.mark_item_as_not_gotten(list_id, item_name, item_index)
        return no_content()
    elif action == 'update-order':
        body = await request.json()
        await driver.reorder_items(list_id, body)
        return no_content()
    else:
        return bad_request()


def gen_etag(data: bytes) -> str:
    digest = base64.urlsafe_b64encode(
        hashlib.sha256(data).digest()
    ).decode('ascii')
    return f'"{digest}"'


def html_response(html: str) -> web.Response:
    body = html.encode('utf-8')
    return web.Response(
        body=body,
        headers={
            'Content-Type': 'text/html; charset=utf-8',
            'ETag': gen_etag(body),
        }
    )


def json_response(v, extra_headers=None) -> web.Response:
    headers = {}
    if extra_headers is not None:
        for hk, hv in extra_headers.items():
            headers[hk] = hv
    headers['Content-Type'] = 'application/json'
    return web.Response(
        body=json.dumps(v).encode('utf-8'),
        headers=headers,
    )


def no_content() -> web.Response:
    return web.Response(status=204)


def bad_request() -> web.Response:
    return problem.problems(400)


def not_found() -> web.Response:
    return problems.problem(404)


class NeedQVar(Exception):
    def __init__(self, missing_var):
        self.missing_var = missing_var


def need_qvar(request, key):
    if key in request.query:
        return request.query[key]
    else:
        raise NeedQVar(key)


def no_such_list() -> web.Response:
    return problems.problem(
        404,
        'no_such_list',
        'That list does not exist.',
    )


def cached_svg(path):
    data = path.read_bytes()
    async def handler(_req):
        return web.Response(
            body=data,
            headers={
                'Content-Type': 'image/svg+xml',
            }
        )
    return handler


def static_js(path):
    async def handler(_req):
        data = path.read_bytes()
        return web.Response(
            body=data,
            headers={
                'Content-Type': 'text/javascript; charset=utf-8',
            }
        )
    return handler


async def load_db_driver(config):
    driver = config['driver']
    driver_type = driver['type']
    if driver_type == 'postgres':
        return await postgres_driver.create_driver(driver)
    else:
        raise NotImplementedError(f'No such database driver {driver_type!r}')


def main():
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument('--bind', action='store', default='::1')
    args = parser.parse_args()

    with open('groceries.toml', 'rb') as fh:
        config = tomllib.load(fh)

    sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0);
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((args.bind, 8080))

    app = web.Application()
    app.add_routes([
        web.get('/', fe_index),
        web.get('/list', fe_list),
        web.get('/api/list', get_list_collection),
        web.get('/api/list/', get_list_collection),
        web.post('/api/list', post_list_collection),
        web.get('/api/list/{list_id}', get_list),
        web.post('/api/list/{list_id}', post_list),
        web.get('/images/caret-up.svg', cached_svg(HTML / 'caret-up.svg')),
        web.get('/images/caret-down.svg', cached_svg(HTML / 'caret-down.svg')),
        web.get('/images/trash.svg', cached_svg(HTML / 'trash.svg')),
        web.get('/images/xmark.svg', cached_svg(HTML / 'xmark.svg')),
        web.get('/favicon.svg', cached_svg(HTML / 'basket-shopping-🌈.svg')),
        web.get('/js/dialogs.js', static_js(HTML / 'dialogs.js')),
    ])

    app.cleanup_ctx.append(db_driver_thunk(config))

    async def amain():
        event = asyncio.Event()

        def ctrl_c():
            import sys
            print('SIGINT received, exiting…', file=sys.stderr)
            event.set()

        import signal
        asyncio.get_event_loop().add_signal_handler(signal.SIGINT, ctrl_c)

        app_runner = web.AppRunner(app)
        await app_runner.setup()
        tcp_site = web.SockSite(app_runner, sock)
        await tcp_site.start()
        logging.info('Server started.')
        await event.wait()
        logging.info('Server ending…')
        await tcp_site.stop()
        await app_runner.shutdown()
        await app_runner.cleanup()
        logging.info('Goodbye.')

    asyncio.run(amain())


def db_driver_thunk(config):
    async def thunk(app):
        logging.debug('Cleanup context begin.')
        db_driver = await load_db_driver(config)
        app['db_driver'] = db_driver
        logging.debug('DB driver created')
        try:
            yield
        finally:
            logging.debug('Cleanup context done.')
        await db_driver.close()
        logging.debug('DB driver closed.')

    return thunk


if __name__ == '__main__':
    main()
