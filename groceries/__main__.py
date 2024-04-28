import argparse
import datetime
import json
import logging
from pathlib import Path
import socket
import tomllib

from aiohttp import web
import aiopg

from . import crdb_driver


HTML = Path(__file__).parent / 'html'


async def hello(request):
    return web.Response(text="Hello, world")


async def fe_list(request):
    p = (HTML / 'list.html').read_bytes()
    return html_response(p)


async def get_list_collection(request):
    driver = request.app['db_driver']
    current = request.query.get('current')
    if current == '':
        list_id = await driver.current_list()
        return json_response(f'/api/list/{list_id}')
    else:
        return bad_request()


async def post_list_collection(request):
    driver = request.app['db_driver']
    action = request.query.get('action')
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
    sequence, items = await driver.get_list(list_id)

    json = []
    for item in items:
        json.append({
            'item_name': item.name,
            'item_index': item.index,
            'in_cart': item.in_cart,
            'purchase_price': item.purchase_price,
        })

    logging.info('%r', items)
    extra_headers = {
        'ETag': f'W/"sequence-{sequence}"',
    }
    return json_response(json, extra_headers=extra_headers)


async def post_list(request):
    list_id = request.match_info['list_id']
    pool = request.app['db_pool']
    action = request.query.get('action')
    if action == 'add-item':
        body = await request.json()
        item_name = body['item_name']
        with (await pool.cursor()) as cur:
            await cur.execute(
                'INSERT INTO grocery_list_items'
                ' (list_id, item_name, item_index, in_cart, purchase_price)'
                ' VALUES (%s, %s, (SELECT max(item_index) + 1 FROM grocery_list_items WHERE list_id = %s), FALSE, NULL);',
                (list_id, item_name, list_id),
            )
            return no_content()
    elif action == 'remove-item':
        body = await request.json()
        item_name = body['item_name']
        item_index = body['item_index']
        with (await pool.cursor()) as cur:
            logging.info(
                'Removing from list: (%r, %r, %r)',
                list_id, item_name, item_index,
            )
            await cur.execute(
                'DELETE FROM grocery_list_items'
                ' WHERE list_id = %s AND item_name = %s AND item_index = %s;',
                (list_id, item_name, item_index),
            )
            return no_content()
    elif action == 'add-to-cart':
        body = await request.json()
        item_name = body['item_name']
        item_index = body['item_index']
        with (await pool.cursor()) as cur:
            logging.info('Adding to cart: (%r, %r, %r)', list_id, item_name, item_index)
            await cur.execute(
                'UPDATE grocery_list_items'
                ' SET in_cart = TRUE'
                ' WHERE list_id = %s AND item_name = %s AND item_index = %s;',
                (list_id, item_name, item_index),
            )
            return no_content()
    elif action == 'remove-from-cart':
        body = await request.json()
        item_name = body['item_name']
        item_index = body['item_index']
        with (await pool.cursor()) as cur:
            logging.info(
                'Removing from cart: (%r, %r, %r)',
                list_id, item_name, item_index,
            )
            await cur.execute(
                'UPDATE grocery_list_items'
                ' SET in_cart = FALSE, purchase_price = NULL'
                ' WHERE list_id = %s AND item_name = %s AND item_index = %s;',
                (list_id, item_name, item_index),
            )
            return no_content()
    elif action == 'update-order':
        body = await request.json()
        with (await pool.cursor()) as cur:
            await reorder(cur, list_id, body)
        return no_content()
    else:
        return bad_request()


async def reorder(cur, list_id, new_order):
    # … it is really hard to re-order an ordinal column.
    # Originally, we tried to do this with a CTE, to ensure that the UNIQUE
    # constraint on the index column would be respected:
    #
    #     WITH new_order AS (
    #       SELECT * FROM (VALUES (…)) AS t(item_name, old_index, new_index)
    #     )
    #     UPDATE grocery_list_items\n'
    #       SET item_index = new_order.new_index
    #       FROM new_order
    #       WHERE grocery_list_items.list_id = %s
    #       AND grocery_list_items.item_name = new_order.item_name
    #       AND grocery_list_items.item_index = new_order.old_index
    #     ;
    #
    # This still violates the unique contraint, even though at no point is the
    # constraint violated, except *during* the execution of a single UPDATE!
    #
    # [This answer](https://dba.stackexchange.com/a/285964/93178) says that the
    # PG docs are wrong, and that in fact, constrains must be valid every time
    # a row is written: i.e., *even in the middle of the statement!* This feels
    # like a flagrant violation of ACID.
    #
    # In PG, the answer is to make the contraint deferrable. But CRDB doesn't
    # support those.
    #
    # So instead, we find the highest index, and re-write the indexes at values
    # higher than than. Then we shift it all back.

    async def run_cte_update(cursor, shifts):
        update_values_sql = []
        update_values_params = []
        for item_name, old_index, new_index in shifts:
            update_values_sql.append('(%s, %s, %s)')
            update_values_params.append(item_name)
            update_values_params.append(old_index)
            update_values_params.append(new_index)
        update_values = ', '.join(update_values_sql)
        sql = (
            'WITH new_order AS (\n'
            f'  SELECT * FROM (VALUES {update_values}) AS t(item_name, old_index, new_index)\n'
            ')\n'
            'UPDATE grocery_list_items\n'
            ' SET item_index = new_order.new_index\n'
            ' FROM new_order\n'
            ' WHERE grocery_list_items.list_id = %s\n'
            ' AND grocery_list_items.item_name = new_order.item_name\n'
            ' AND grocery_list_items.item_index = new_order.old_index\n'
            ';'
        )
        update_values_params.append(list_id)
        logging.info('Update order:')
        logging.info('  query:\n%s', sql)
        logging.info('  params: %r', update_values_params)
        await cur.execute(sql, update_values_params)

    async with cur.begin():
        await cur.execute(
            'SELECT max(item_index)'
            ' FROM grocery_list_items'
            ' WHERE list_id = %s;',
            list_id,
        )
        highest_id, = await cur.fetchone()

        shifts = []
        for new_index, (item_name, old_index) in enumerate(new_order):
            shifts.append((item_name, old_index, new_index + highest_id + 1))
        await run_cte_update(cur, shifts)

        await cur.execute(
            'UPDATE grocery_list_items'
            ' SET item_index = item_index - %s'
            ' WHERE list_id = %s;',
            (highest_id + 1, list_id),
        )

def html_response(d) -> web.Response:
    return web.Response(
        body=d,
        headers={
            'Content-Type': 'text/html; charset=utf-8',
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
    return web.Response(
        status=400,
        body=b'{}',
        headers={
            'Content-Type': 'application/problem+json',
        }
    )


def not_found() -> web.Response:
    return web.Response(
        status=404,
        body=b'{}',
        headers={
            'Content-Type': 'application/problem+json',
        }
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


def load_db_driver(config):
    driver = config['driver']
    driver_type = driver['type']
    if driver_type == 'cockroachdb':
        return crdb_driver.create_driver(driver)
    else:
        raise NotImplementedError(f'No such database driver {driver_type!r}')


def main():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('--bind', action='store', default='::1')
    args = parser.parse_args()

    with open('groceries.toml', 'rb') as fh:
        config = tomllib.load(fh)

    db_driver = load_db_driver(config)
    logging.info('Database driver: %r', db_driver)

    sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0);
    sock.bind((args.bind, 8080))

    app = web.Application()
    app.add_routes([
        web.get('/', hello),
        web.get('/list', fe_list),
        web.get('/api/list', get_list_collection),
        web.post('/api/list', post_list_collection),
        web.get('/api/list/{list_id}', get_list),
        web.post('/api/list/{list_id}', post_list),
        web.get('/images/caret-up.svg', cached_svg(HTML / 'caret-up.svg')),
        web.get('/images/caret-down.svg', cached_svg(HTML / 'caret-down.svg')),
        web.get('/images/trash.svg', cached_svg(HTML / 'trash.svg')),
        web.get('/images/xmark.svg', cached_svg(HTML / 'xmark.svg')),
        web.get('/js/dialogs.js', static_js(HTML / 'dialogs.js')),
    ])
    app.cleanup_ctx.append(db_pool)
    app.cleanup_ctx.append(db_driver_thunk(db_driver))
    web.run_app(app, sock=sock)


def db_driver_thunk(db_driver):
    async def thunk(app):
        app['db_driver'] = db_driver
        yield

    return thunk 


def get_db_dsn_and_args():
    dsn = 'dbname=groceries user=groceries host=localhost port=26257 sslmode=verify-full'
    certs = Path('/') / 'home' / 'royiv' / 'code' / 'ci' / 'cockroach-v22.2.19.linux-amd64' / 'cockroach-certs'
    kwargs = {
        'sslmode': 'verify-full',
        'sslcert': certs / 'client.groceries.crt',
        'sslkey': certs / 'client.groceries.key',
        'sslrootcert': certs / 'ca.crt',
    }

    return dsn, kwargs


async def db_pool(app):
    dsn, kwargs = get_db_dsn_and_args()
    app['db_pool'] = MockPool(dsn, kwargs)
    yield


#async def db_pool(app):
#    logging.info('Creating pool.')
#    dsn = 'dbname=groceries user=groceries host=localhost port=26257 sslmode=verify-full'
#    certs = Path('/') / 'home' / 'royiv' / 'code' / 'ci' / 'cockroach-v22.2.19.linux-amd64' / 'cockroach-certs'
#    logging.info('path exists? %s', certs.exists())
#    kwargs = {
#        'sslmode': 'verify-full',
#        'sslcert': certs / 'client.groceries.crt',
#        'sslkey': certs / 'client.groceries.key',
#        'sslrootcert': certs / 'ca.crt',
#    }
#    async with aiopg.create_pool(dsn, **kwargs) as pool:
#        logging.info('Pool ready.')
#        app['db_pool'] = pool
#        yield
#
#
#async def test_db_pool():
#    logging.info('Creating test connection.')
#    dsn = 'dbname=groceries user=groceries host=localhost port=26257 sslmode=verify-full'
#    certs = Path('/') / 'home' / 'royiv' / 'code' / 'ci' / 'cockroach-v22.2.19.linux-amd64' / 'cockroach-certs'
#    logging.info('path exists? %s', certs.exists())
#    kwargs = {
#        'sslmode': 'verify-full',
#        'sslcert': certs / 'client.groceries.crt',
#        'sslkey': certs / 'client.groceries.key',
#        'sslrootcert': certs / 'ca.crt',
#    }
#    async with aiopg.connect(dsn, **kwargs) as pool:
#        logging.info('Connection ready.')


class MockPool:
    def __init__(self, dsn, conn_kwargs):
        self.dsn = dsn
        self.conn_kwargs = conn_kwargs
        import psycopg2
        self.psycopg2 = psycopg2

    async def cursor(self):
        conn = self.psycopg2.connect(self.dsn, **dict(self.conn_kwargs))
        return MockCursor(conn)


class MockCursor:
    def __init__(self, connection):
        self.connection = connection
        self.cursor = connection.cursor()
        self.in_transaction = False

    def __enter__(self):
        self.cursor.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cursor.__exit__(exc_type, exc_value, traceback)
        self.connection.close()

    def begin(self):
        return MockTransaction(self, self.connection, self.cursor)

    async def execute(self, query, params):
        result = self.cursor.execute(query, params)
        if not self.in_transaction:
            self.connection.commit()
        return result

    async def fetchall(self):
        return self.cursor.fetchall()

    async def fetchone(self):
        return self.cursor.fetchone()


class MockTransaction:
    def __init__(self, mock_cursor, connection, cursor):
        self.mock_cursor = mock_cursor
        self.connection = connection
        self.cursor = cursor

    async def __aenter__(self):
        self.mock_cursor.in_transaction = True
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.connection.commit()
        else:
            self.connection.rollback()
        self.mock_cursor.in_transaction = False


if __name__ == '__main__':
    main()
