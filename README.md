# MongoDB Foreign Data Wrapper for PostgreSQL

This [MongoDB][1] extension implements the PostgreSQL's Foreign Data Wrapper.

Please note that this version of mongo_fdw works with PostgreSQL and EDB Postgres Advanced Server 9.3, 9.4, 9.5 and 9.6.

Installation
------------
The MongoDB FDW depends on the official MongoDB C Driver version 0.8  and includes it as a git submodule. If you are cloning this repository for the first time, be sure to pass the --recursive option to git clone in order to initialize the driver submodule to a useable state.

If checked out this project before and for some reason your submodule is not up-to-date, run git submodule update --init.

When you type `make`, the C driver's source code also gets automaticallycompiled and linked.

Note: Make sure you have permission to "/usr/local" (default installation location) folder.

Note that we have verified the `mongo_fdw` extension only on MacOS X,
Fedora and Ubuntu systems. If you run into issues on other systems, please [let us know][3]

Enhancements
-----------
The following enhancements are added to the latest version of mongo_fdw

Write-able FDW
--------------
The previous version was only read-only, the latest version provides the write capability.
The user can now issue an insert / update and delete statements for the foreign tables using the mongo_fdw.

Connection Pooling
------------------
The latest version comes with a connection pooler that utilizes the same mango database connection for all the queries in the same session. The previous version would open a new [MongoDB][1] connection for every query. This is a performance enhancement.

New MongoDB C Driver Support
----------------------------
The third enhancement is to add a new [MongoDB][1]' C driver.

Installation
------------

  * download the MongoDB C driver with libbson: https://github.com/mongodb/mongo-c-driver
```
wget https://github.com/mongodb/mongo-c-driver/releases/download/1.9.2/mongo-c-driver-1.9.2.tar.gz
tar -xf mongo-c-driver-*.tar.gz
cd mongo-c-driver-*/
```
  * install `libbson` specifying `--with-libbson=system`
```
cd src/libbson/
./configure --with-libbson=system
checkinstall -y make install
```
  * install `libmongoc`
```
cd ../../
./configure
checkinstall -y make install
```
  * install `mongo_fdw` with `make -f Makefile.meta && make -f Makefile.meta install`
```
git clone git@github.com:siilike/mongo_fdw.git
cd mongo_fdw
make -f Makefile.meta
checkinstall -y make -f Makefile.meta install
```
  * if you get an error when trying to `CREATE EXTENSION mongo_fdw;`, then try running `ldconfig`

Usage
-----
The following parameters can be set on a MongoDB foreign server object:

  * **`address`**: the address or hostname of the MongoDB server Defaults to `127.0.0.1`
  * **`port`**: the port number of the MongoDB server. Defaults to `27017`
  * **`authentication_database`**: database against which user will be authenticated against. Only valid with password based authentication. Defaults to per same database as the MongoDB collection database.
  * **`replica_set`**: replica set the server is member of. If set, driver will auto-connect to correct primary in the replica set when writing.  
  * **`read_preference`**: primary [default], secondary, primaryPreferred, secondaryPreferred, or nearest (meta driver only).  Defaults to `primary`
  * **`ssl`**: false [default], true to enable ssl (meta driver only). See http://mongoc.org/libmongoc/current/mongoc_ssl_opt_t.html to understand the options.
  * **`pem_file`**: SSL option;
  * **`pem_pwd`**: SSL option;
  * **`ca_file`**: SSL option;
  * **`ca_dir`**: SSL option;
  * **`crl_file`**: SSL option;
  * **`weak_cert_validation`**: SSL option;

The following parameters can be set on a MongoDB foreign table object:

  * **`database`**: the name of the MongoDB database to query. Defaults to `test`
  * **`collection`**: the name of the MongoDB collection to query. Defaults to the foreign table name used in the relevant `CREATE` command

As an example, the following commands demonstrate loading the `mongo_fdw`
wrapper, creating a server, and then creating a foreign table associated with
a MongoDB collection. The commands also show specifying option values in the
`OPTIONS` clause. If an option value isn't provided, the wrapper uses the
default value mentioned above.

`mongo_fdw` can collect data distribution statistics will incorporate them when estimating costs for the query execution plan. To see selected execution plans for a query, just run `EXPLAIN`.

Examples with [MongoDB][1]'s equivalent statments.

```sql

-- load extension first time after install
CREATE EXTENSION mongo_fdw;

-- create server object
CREATE SERVER mongo_server
         FOREIGN DATA WRAPPER mongo_fdw
         OPTIONS (address '127.0.0.1', port '27017');

-- create user mapping
CREATE USER MAPPING FOR postgres
		 SERVER mongo_server
		 OPTIONS (username 'mongo_user', password 'mongo_pass');

-- create foreign table
CREATE FOREIGN TABLE warehouse(
		 _id NAME,
         warehouse_id int,
         warehouse_name text,
         warehouse_created timestamptz)
SERVER mongo_server
         OPTIONS (database 'db', collection 'warehouse');

-- Note: first column of the table must be "_id" of type "NAME".

-- select from table
SELECT * FROM warehouse WHERE warehouse_id = 1;

           _id          | warehouse_id | warehouse_name |     warehouse_created
------------------------+----------------+---------------------------
53720b1904864dc1f5a571a0|            1 | UPS            | 12-DEC-14 12:12:10 +05:00


db.warehouse.find({"warehouse_id" : 1}).pretty()
{
	"_id" : ObjectId("53720b1904864dc1f5a571a0"),
	"warehouse_id" : 1,
	"warehouse_name" : "UPS",
	"warehouse_created" : ISODate("2014-12-12T07:12:10Z")
}


-- insert row in table
INSERT INTO warehouse values (0, 1, 'UPS', '2014-12-12T07:12:10Z');

db.warehouse.insert
(
    {
        "warehouse_id" : NumberInt(1),
        "warehouse_name" : "UPS",
        "warehouse_created" : ISODate("2014-12-12T07:12:10Z")
    }
);

-- delete row from table
DELETE FROM warehouse where warehouse_id = 3;

>    db.warehouse.remove({"warehouse_id" : 2})


-- update a row of table
UPDATE warehouse set warehouse_name = 'UPS_NEW' where warehouse_id = 1;

db.warehouse.update
(
   {
        "warehouse_id" : 1
   },
   {
        "warehouse_id" : 1,
        "warehouse_name" : "UPS_NEW"
   }
)

-- explain a table
EXPLAIN SELECT * FROM warehouse WHERE warehouse_id = 1;
                           QUERY PLAN
 -----------------------------------------------------------------
 Foreign Scan on warehouse  (cost=0.00..0.00 rows=1000 width=44)
   Filter: (warehouse_id = 1)
   Foreign Namespace: db.warehouse
 Planning time: 0.671 ms
(4 rows)

-- collect data distribution statistics`
ANALYZE warehouse;

```

Query mapping
-------------
Simple queries that do not modify fields are mapped directly to MongoDB queries, for instance:

```sql
SELECT * FROM table;
SELECT * FROM table WHERE field1 IS NULL;
SELECT * FROM table WHERE field1 IS NOT NULL;
SELECT * FROM table WHERE field1 = 5;
SELECT * FROM table WHERE field1 = 5 AND field2 = 'a';
SELECT * FROM table WHERE field1 IN(5,6,7);
SELECT * FROM table WHERE field1 NOT IN(5,6,7);  
SELECT * FROM table WHERE field1 = ANY(ARRAY[1,2,3]);
SELECT * FROM table WHERE field1 IN(5,6,7) OR field2 = 'a';
SELECT * FROM table WHERE (field1 = 5 AND field2 = 'a') OR (field1 = 6 AND field2 = 'b');
```
but these are not:
```sql
SELECT * FROM table WHERE field1 = 5 AND LOWER(field2) = 'a';
SELECT * FROM table WHERE field1 = 5 OR LOWER(field2) = 'a';
```
The former one gets executed as:
```sql
SELECT * FROM table WHERE field1 = 5;
```
and then filtered for ```LOWER(field2) = 'a'``` in PostgresSQL, basically it is the eqivalent of:
```sql
WITH a AS (
    SELECT * FROM table WHERE field1 = 5
)
SELECT * FROM a WHERE LOWER(field2) = 'a';
```
The latter one selects the whole table and performs all filtering in PostgresSQL. A warning is generated if any of the expressions cannot be mapped directly.

`UPDATE` queries the rows first and then updates each of them separately by `_id`.

Limitations
-----------

  * If the BSON document key contains uppercase letters or occurs within a
    nested document, `mongo_fdw` requires the corresponding column names to be
	declared in double quotes.

  * Note that PostgreSQL limits column names to 63 characters by default. If
    you need column names that are longer, you can increase the `NAMEDATALEN`
	constant in `src/include/pg_config_manual.h`, compile, and reinstall.


Contributing
------------
Have a fix for a bug or an idea for a great new feature? Great! Check out the contribution guidelines [here][4]. For all other types of questions or comments about the wrapper please contact us at `mongo_fdw` `@` `enterprisedb.com`.


Support
-------
This project will be modified to maintain compatibility with new PostgreSQL and EDB Postgres Advanced Server releases.

If you need commercial support, please contact the EnterpriseDB sales team, or check whether your existing PostgreSQL support provider can also support mongo_fdw.

License
-------
Portions Copyright © 2004-2016, EnterpriseDB Corporation.

Portions Copyright © 2012–2014 Citus Data, Inc.

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as published by the Free
Software Foundation, either version 3 of the License, or (at your option) any later version.

See the [`LICENSE`][5] file for full details.

[1]: http://www.mongodb.com
[2]: http://www.citusdata.com/blog/51-run-sql-on-mongodb
[3]: https://github.com/enterprisedb/mongo_fdw/issues/new
[4]: CONTRIBUTING.md
[5]: LICENSE
[6]: https://github.com/mongodb/mongo-c-driver-legacy
[7]: https://github.com/mongodb/mongo-c-driver
