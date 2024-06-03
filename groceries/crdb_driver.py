import contextlib
import logging
from pathlib import Path
from typing import Tuple

import psycopg_pool

from .obj_model import GroceryList, ListItem


class NoSuchList(Exception):
    pass


async def create_driver(toml_config):
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
    logging.debug('CRDB driver, database kwargs = %r', kwargs)
    import psycopg
    c = await psycopg.AsyncConnection.connect(dsn, **kwargs)
    print(f'c = {c!r}')

    pool = psycopg_pool.AsyncConnectionPool(
        dsn,
        kwargs=kwargs,
        open=False,
        min_size=0,
        max_size=4,
    )
    logging.debug('Pool opening.')
    await pool.open()
    logging.debug('Pool open.')
    return CrdbDriver(pool)


class CrdbDriver:
    def __init__(self, pool):
        self.pool = pool

    async def close(self):
        await self.pool.close()

    #async def advance

    def _parse_list_id(self, list_id: str) -> int:
        try:
            return int(list_id)
        except ValueError:
            raise NoSuchList()

    @contextlib.asynccontextmanager
    async def _in_transaction(self):
        async with self.pool.connection() as conn:
            async with conn.transaction() as transaction:
                async with conn.cursor() as cur:
                    yield cur

    async def new_list(self, created_at):
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    'INSERT INTO grocery_lists'
                    ' (created_at) VALUES (%s)'
                    ' RETURNING list_id;',
                    (created_at,),
                )
                results = await cur.fetchall()
                return results[0][0]

    async def current_list(self):
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    'SELECT list_id'
                    ' FROM grocery_lists'
                    ' ORDER BY created_at DESC'
                    ' LIMIT 1;',
                    tuple(),
                )
                results = await cur.fetchall()
                return results[0][0]

    async def get_lists(self):
        async with self._in_transaction() as cur:
            await cur.execute(
                'SELECT list_id'
                ' FROM grocery_lists'
                ' ORDER BY created_at DESC;',
                tuple(),
            )
            results = await cur.fetchall()
        return [str(r[0]) for r in results]

    async def get_list(self, list_id) -> Tuple[int, GroceryList]:
        list_id = self._parse_list_id(list_id)
        async with self._in_transaction() as cur:
            await cur.execute(
                'SELECT sequence, created_at'
                ' FROM grocery_lists'
                ' WHERE list_id = %s;',
                (list_id,),
            )
            row = (await cur.fetchall())[0]
            sequence_num, created_at = row
            await cur.execute(
                'SELECT item_name, item_index, in_cart, purchase_price'
                ' FROM grocery_list_items'
                ' WHERE list_id = %s'
                ' ORDER BY item_index;',
                (list_id,),
            )
            raw_items = await cur.fetchall()

        def map_row(row):
            item_name, item_index, in_cart, purchase_price = row
            return ListItem(item_name, item_index, in_cart, purchase_price)

        items = [map_row(row) for row in raw_items]
        grocery_list = GroceryList(created_at=created_at, items=items)
        return sequence_num, grocery_list

    async def add_item(self, list_id, item_name):
        list_id = self._parse_list_id(list_id)
        async with self.pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        'INSERT INTO grocery_list_items'
                        ' (list_id, item_name, item_index, in_cart, purchase_price)'
                        ' VALUES (%s, %s, (SELECT coalesce(max(item_index) + 1, 0) FROM grocery_list_items WHERE list_id = %s), FALSE, NULL);',
                        (list_id, item_name, list_id),
                    )

    async def remove_item(self, list_id, item_name, item_index):
        list_id = self._parse_list_id(list_id)
        async with self.pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        'DELETE FROM grocery_list_items'
                        ' WHERE list_id = %s AND item_name = %s AND item_index = %s;',
                        (list_id, item_name, item_index),
                    )

    async def mark_item_as_gotten(self, list_id, item_name, item_index):
        list_id = self._parse_list_id(list_id)
        async with self.pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        'UPDATE grocery_list_items'
                        ' SET in_cart = TRUE'
                        ' WHERE list_id = %s AND item_name = %s AND item_index = %s;',
                        (list_id, item_name, item_index),
                    )

    async def mark_item_as_not_gotten(self, list_id, item_name, item_index):
        list_id = self._parse_list_id(list_id)
        async with self.pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        'UPDATE grocery_list_items'
                        ' SET in_cart = FALSE, purchase_price = NULL'
                        ' WHERE list_id = %s AND item_name = %s AND item_index = %s;',
                        (list_id, item_name, item_index),
                    )

    async def reorder_items(self, list_id, new_order):
        list_id = self._parse_list_id(list_id)
        # `new_order` is a list of `(item_name, item_index)` items, in the
        # desired order for the new order.
        async with self.pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await reorder(cur, list_id, new_order)


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
            update_values_sql.append('(%s::text, %s, %s)')
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

    await cur.execute(
        'SELECT max(item_index)'
        ' FROM grocery_list_items'
        ' WHERE list_id = %s;',
        (list_id,),
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
