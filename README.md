[![On-commit](https://github.com/Dmitry-k42/flow-sql/actions/workflows/on-commit.yml/badge.svg)](https://github.com/Dmitry-k42/flow-sql/actions/workflows/on-commit.yml)

# Build SQL queries fluently

`sqlcraft` is a Python package for constructing SQL queries with [fluent interface](https://en.wikipedia.org/wiki/Method_chaining).

**Important**: Currently the package supports PostgreSQL database **only**.

# SELECT

Here is a simple query:
```python
import psycopg2
from sqlcraft import Query

conn = psycopg2.connect(<put your connection credentials here>)
cmd = (
    Query(conn)
    .select(['id', 'name', 'age'])
    .from_('users')
    .where({
        'id': 42,
        'name': 'Jane',
    })
    .order('id')
)
```
Get resulting query text with `cmd.as_string()` method:
```SQL
SELECT "id", "name", "age" FROM "users" WHERE ("id" = 42) AND ("name" = 'Jane') ORDER BY "id"
```

Surely all the values will be passed as query parameters in order to prevent
[SQL injection](https://www.google.com/url?sa=t&rct=j&q=&esrc=s&source=web&cd=&cad=rja&uact=8&ved=2ahUKEwiu3OPy7sT7AhXNSPEDHUuhC88QFnoECEcQAQ&url=https%3A%2F%2Fen.wikipedia.org%2Fwiki%2FSQL_injection&usg=AOvVaw1uzN238Ma4U7MFZg35w2eA).
Here the resulting query is illustrated with values inside for better understanding.

## Data fetching
When you have built the query, use one of methods `.all()`, `column()`, `.one()`
or `.scalar()`. Method `.all()` allows you iterate over all the resulting rows from the database.
By default it returns a generator. Set parameter `eager=True` to get all the rows as an array.
Use `.column()` to fetch only a single column, `.one()` to fetch a single row and `.scalar()`
to fetch just one value. Example:
```python
for row in cmd.all():
    id, name, age = row
```
Get count of rows in the table:
```python
users_total = Query(conn).select('count(*)').from_('users').scalar()
```

## Chaining conditions

Using method chaining you can easily combine different conditions in the WHERE clause.
Some examples:

```python
Query(conn)
    ...
    .and_where({'gender': ['male', 'female']})
    .and_where('active')
    .and_where('NOT banned')
    .and_where(['BETWEEN', 'age', 18, 25])
    .and_where(['!=', 'name', 'Aaron'])
    .and_where('credit = debit')
    ...
```
This code will produce
```SQL
WHERE ("gender" IN ('male', 'female')) AND ("active") AND ("age" BETWEEN 18 AND 25) AND ("name" != 'Aaron') AND ("credit" = "debit")
```

You can easily combine AND'n'OR conditions:
```python
Query(conn)
    .where([
        'OR',
        [
            'AND',
            ['>', 'age', 35],
            ['!=', 'name', 'Donald'],
        ],
        [
            'AND',
            ['<', 'age', 18],
            'NOT active',
        ],
    ])
```
Result:
```SQL
WHERE (("age" > 35) AND ("name" != 'Donald') OR (("age" < 18) AND (NOT "active"))
```

Quoting is on by default. Builder will surround every field or table name with `"` (for PostgreSQL)
if the string doesn't contain `(` and/or `)` symbols. If you want to turn off quoting manually,
just embrace identifier with brackets. Examples:
```python
Query(conn)
    ...
    .and_where('price > 10')         # "price" > 10       <- Quoting in on by default
    .and_where('(no_quoting) > 10')  # (no_quoting) > 10  <- Quoting turned off manually
    .and_where('sum("price") > 10')  # sum("price") > 10  <- You should do quoting here manually!
    ...
```

### Joins
Table joining are provided with methods `.join()`, `.join_left()`, `.join_right()`,
`.join_inner()` (synonim to `.join()`) and `.join_full()`. Example:
```python
Query(conn)
    .select(['users.id', 'wallets.balance', 'l.lat', 'l.lon'])
    .from_('users')
    .join('wallets', on='users.id=wallets.user_id')
    .join_left(
        'location',
        alias='l',  # Table aliasing is available with `alias` argument
        on='users.id=l.user_id'
    )
```

### Subqueries
You can combine queries together when you need to have more complicated queries:
```python
subq = Query(conn).select('*').from_('devices').where('active')

# Subquery in FROM clause
(
    Query(conn)
    .select(['d.id', 'd.imei'])
    .from_(subq, alias='d')
)

# Use CTE in WITH and JOIN clauses
(
    Query(conn)
    .with_(subq, alias='sq')
    ...
    .from_('user', alias='u')
    .join_('sq', on='u.id=sq.user_id')
    ...
)

# Subquery in WHERE clause
(
    Query(conn)
    ...
    .and_where([
        'EXISTS',
        'device_id',
        Query(conn).select('id').from_('devices')
    ])
    ...
)
```

# INSERT
One statement usage:
```python
from sqlcraft import Insert

Insert(
    conn,
    table='users',
    values=[
        {'id': 42, 'name': 'Viktor', 'weight': 75.27, 'active': True, 'pet': None},
        {'id': 43, 'name': 'Anna', 'weight': 56.76, 'active': False, 'pet': 'kitty'},
    ]
).execute()
```

Another syntax:
```python
Insert(
    conn,
    table='users',
    columns=['id', 'name', 'weight', 'active', 'pet'],
    values=[
        [42, 'Viktor', 75.27, True, None],
        [43, 'Anna', 56.76, False, 'kitty'],
    ]
).execute()
```

Fluent syntax:
```python
(
    Insert(conn)
    .table('users')
    .columns(['id', 'name', 'weight', 'active', 'pet'])
    .add_values([42, 'Viktor', 75.27, True, None])
    .add_values([43, 'Anna', 56.76, False, 'kitty'])
    .execute()
)
```

RETURNING clause is available:
```python
cmd = Insert(
    conn,
    table='users',
    values={'name': 'Viktor'},
)
viktor_id = cmd.returning('id').scalar()
```

PostgreSQL extends standard syntax for `INSERT` command. It allows to handle conflicts with special
`ON CONFLICT` keyword. For more information, please see [here](https://www.postgresql.org/docs/current/sql-insert.html)
("ON CONFLICT Clause" section). To create this clause you can use `.on_conflict_do_nothing()`
and `.on_conflict_do_update()` methods.
```python
from sqlcraft import Expr

(
    # If another row in the table with id 42 already exists,
    # a new row will be ignored
    Insert(conn)
    .table('users')
    .columns(['id', 'name'])
    .add_values([42, 'Viktor'])
    .on_conflict_do_nothing('id')
    .execute()
)

(
    # If another row in the table with id 42 already exists,
    # then `name` column of existing row will be updated with a new value
    Insert(conn)
    .table('users')
    .columns(['id', 'name'])
    .add_values([42, 'Viktor'])
    .on_conflict_do_update(
        'id',
        {
            'name': Expr('EXCLUDED.name'),
        },
    )
    .execute()
)
```
Please take a look at `Expr` class. It tells the builder not to quote and not to parametrize
its content. This is a useful tool when you want to pass a part of SQL query as is. You should know
what you are doing with `Expr` because burden of protection against SQL injections lays on
your shoulders. **NEVER use potentially unsafe data as an argument of `Expr`!** In this case
we know what we are doing. Without `Expr` running this command could replace
`name` field with a string "EXCLUDED.name" instead of "Viktor". Parametrizing works always by
default. This is surely good but actually not a thing we want to get in this case. This is why we wrap "EXCLUDED.name"
value with calling `Expr` in order to tell the builder that "EXCLUDED.name" is a part of the query.

It is possible to use `INSERT...SELECT` construction - just pass pre-built `Query` object as an
argument:
```python
cmd = Query(conn).select('id, name, gender').from_('users_src')
Insert(conn, table='users_dst', values=cmd).execute()
```
Resulting query:
```SQL
INSERT INTO "users_dst" SELECT "id", "name", "gender" FROM "users_src"
```

# UPDATE
Simple syntax:
```python
Update(
    conn,
    table='users',
    fields={
        'name': 'New name',
    },
    where={
        'id': 1,
    },
).execute()
```

Fluent syntax:
```python
(
    Update(conn)
    .table('users')
    .set({'name': 'New name'})
    .add_set('age', Expr('age + 1'))
    .where({'id': 1})
    .and_where(['!=', 'name', 'Johnny'])
    .returning('id')
    .column(eager=True)
)
```

# DELETE

Simple syntax:
```python
from sqlcraft import Delete

Delete(
    conn,
    from_='users',
    where={'id': 1},
).execute()
```

The same using method chaining:
```python
(
    Delete(conn)
    .from_('users')
    .where({'id': 1})
    .execute()
)
```

Note that you can not execute `DELETE` queries without specified `WHERE` condition.
The builder raises an exception:

```Exception: Sorry empty WHERE block is restricted on DELETE operations for security reasons```

To remove all rows from the table, you should explicitly call `where` method like this:
```python
(
    Delete(conn)
    .from_('users')
    .where(True)  # The way how to tell the builder that deleting all the rows is ok
    .execute()
)
```

# COPY

`COPY` command is a good choise for bulk insert lots of rows into a table. Here is how to use
it with `sqlcraft`:
```python
from sqlcraft import Copy

Copy(
    conn,
    table='users',
    columns=('id', 'name', 'age'),
    rows=[
        (1, 'Chris', 37),
        (2, 'Brenda', 43),
        (3, 'Mary', 22),
    ]
).execute()
```

# Raw queries

Above the common most-used queries were described. But sometimes we face unusual queries.
It is possible to call raw queries with `Command` class:

```python
from sqlcraft import Command

Command(
    conn,
    'ALTER TABLE "users" DROP COLUMN "gender"'
).execute()
```

Use `%(param_name)s` syntax for passing parameters to your query.
Example of using parametrization with `Command`:
```python
(
    Command(
        conn,
        'SELECT * FROM "users" WHERE "id" = %(id)s'
    )
    .params({
        'id': 1,
    })
    .execute()
)
```

# Advanced PostgreSQL connection

This package provides a custom `Connection` class which replaces
`psycopg2` connection. You can use it if set argument `connection_factory`
this way:
```python
import psycopg2
from sqlcraft.conn import Connection

conn = psycopg2.connect(
    <put your connection credentials here>,
    connection_factory=Connection,
)
```

`Connection` has a few advantages over the standard `psycopg2` connection:

  * **Multi-layered transactions**

    It is available to enclose a transaction into another one. The level of enclosed
    transactions is limited only by your database.

    ```python
    # Open the first transaction
    conn.begin_transaction()
    # Do something here. This will be commited
    ...
    # Open the inner transaction
    conn.begin_transaction()
    # Do database calls. These calls will be rolled back
    ...
    # The inner transaction canceled
    conn.rollback()
    # Do some calls again. This will be commited
    ...
    # Commit the first transaction
    conn.commit()
    ```

  * **Dict-like fetched data**

    With `Connection` class method `.all()` returns data rows has type `DBRow`.
    The `DBRow` implements a few more ways to access the resulting data. Example:
    ```python
    cmd = Query(conn).select('id, name').from_('users')
    for row in cmd:
        # row is a tuple-like object
        id, name = row
        # row is a dict-like object
        id = row['id']
        name = row['name']
        # access to the values via properties
        row.id
        row.name
    ```
