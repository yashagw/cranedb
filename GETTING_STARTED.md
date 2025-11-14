# Getting Started with CraneDB

## Build

```bash
git clone https://github.com/yashagw/cranedb.git
cd cranedb
make build
```

## Run Server

```bash
make run-server
```

Server starts on port `8080` by default.

To use a different port and database dir:
```bash
PORT=8082 DB_DIR=./cranedb_data make run-server
```

## Run Client

In a new terminal:

```bash
make run-client
```

To connect to a different port:
```bash
CRANEDB_PORT=8082 make run-client 
```

## Supported SQL

### Data Types
- `INT` - 32-bit integer
- `VARCHAR(n)` - Variable-length string

### Statements
- `CREATE TABLE` - Create a table
- `CREATE INDEX` - Create an index
- `INSERT INTO` - Insert records
- `SELECT` - Query data
- `UPDATE` - Modify records
- `DELETE` - Remove records

### WHERE Clause
- Only `=` operator supported
- `AND` for multiple conditions
- No `OR`, `<`, `>`, `<=`, `>=`, `!=` yet

## Example Commands

```sql
-- Create table
CREATE TABLE users (id INT, name VARCHAR(20), age INT);

-- Insert data
INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);
INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);

-- Query
SELECT id, name, age FROM users;
SELECT name FROM users WHERE id = 2;

-- Update
UPDATE users SET age = 26 WHERE name = 'Alice';

-- Delete
DELETE FROM users WHERE id = 2;

-- Join (use Cartesian product with WHERE)
CREATE TABLE orders (user_id INT, product VARCHAR(20));
INSERT INTO orders (user_id, product) VALUES (1, 'Laptop');

SELECT name, age, product FROM users, orders WHERE id = user_id;

-- Index
CREATE INDEX users_age_idx ON users (age);
SELECT name, age FROM users WHERE age = 25;
```

## Tips

- Exit client: Type `QUIT` or press Ctrl+C
- Strings use single quotes: `'value'`
- All fields required in INSERT
- Data is persistent across restarts

## More Info

- See `README.md` for architecture details
- Run `make test` to run the test suite
