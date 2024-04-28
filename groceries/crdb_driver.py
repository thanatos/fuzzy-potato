from pathlib import Path
from typing import List

import aiopg

from .obj_model import ListItem


def create_driver(toml_config):
    dsn = toml_config['dsn']

    port = toml_config.get('port', 26257)

    client_crt = Path(toml_config['client-crt'])
    client_key = Path(toml_config['client-key'])
    ca_crt = Path(toml_config['ca-crt'])

    kwargs = {
        'port': port,
        'sslmode': 'verify-full',
        'sslcert': client_crt,
        'sslkey': client_key,
        'sslrootcert': ca_crt,
    }
    pool = MockPool(dsn, kwargs)
    return CrdbDriver(pool)


class CrdbDriver:
    def __init__(self, pool):
        self.pool = pool

    #async def advance

    async def new_list(self, created_at):
        with (await self.pool.cursor()) as cur:
            await cur.execute(
                'INSERT INTO grocery_lists'
                ' (created_at) VALUES (%s)'
                ' RETURNING list_id;',
                (created_at,),
            )
            results = await cur.fetchall()
            return results[0][0]

    async def current_list(self):
        with (await self.pool.cursor()) as cur:
            await cur.execute(
                'SELECT list_id'
                ' FROM grocery_lists'
                ' ORDER BY created_at DESC'
                ' LIMIT 1;',
                tuple(),
            )
            results = await cur.fetchall()
        return results[0][0]

    async def get_list(self, list_id) -> List[ListItem]:
        with (await self.pool.cursor()) as cur:
            async with cur.begin():
                await cur.execute(
                    'SELECT sequence'
                    ' FROM grocery_lists'
                    ' WHERE list_id = %s;',
                    (list_id,),
                )
                sequence_num = await cur.fetchall()
                await cur.execute(
                    'SELECT item_name, item_index, in_cart, purchase_price FROM grocery_list_items WHERE list_id = %s ORDER BY item_index;',
                    list_id,
                )
                results = await cur.fetchall()

        def map_row(row):
            item_name, item_index, in_cart, purchase_price = row
            return ListItem(item_name, item_index, in_cart, purchase_price)

        return sequence_num[0][0], [map_row(row) for row in results]


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


