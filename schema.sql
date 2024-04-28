CREATE SEQUENCE grocery_lists_id_seq;

CREATE TABLE grocery_lists (
	list_id int4 PRIMARY KEY DEFAULT nextval('grocery_lists_id_seq'),
	-- Age / history of the list. Increments on change, to let clients know
	-- their view is out of date.
	sequence int4 NOT NULL DEFAULT 0,
	created_at TIMESTAMP NOT NULL,
);

CREATE TABLE grocery_list_items (
	list_id int4 NOT NULL REFERENCES grocery_lists,
	item_name TEXT NOT NULL,
	item_index INT2 NOT NULL,
	in_cart BOOL NOT NULL,
	purchase_price DECIMAL(7, 2),
	PRIMARY KEY (list_id, item_name),
	UNIQUE (list_id, item_index) DEFERRABLE
);

GRANT CONNECT ON DATABASE groceries TO groceries;
GRANT SELECT, UPDATE ON SEQUENCE grocery_lists_id_seq TO groceries;
GRANT SELECT, INSERT, UPDATE, DELETE ON grocery_lists TO groceries;
GRANT SELECT, INSERT, UPDATE, DELETE ON grocery_list_items TO groceries;
