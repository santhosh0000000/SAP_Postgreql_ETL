# SAP_Postgreql_ETL
This Python script connects to a SAP HANA database, retrieves data from a specified table, and then transfers this data to a PostgreSQL database.


The script first establishes a connection to the SAP HANA database using the pyhdb library and executes a SQL query to retrieve all data from the specified table. It extracts the column names and rows from the query results. After fetching the data, it closes the SAP HANA connection.

Next, the script connects to a PostgreSQL database using the psycopg2 library. It dynamically constructs a SQL query to create a new table in the PostgreSQL database that mirrors the structure of the SAP HANA table. It then generates an INSERT statement and executes it to insert all of the SAP HANA data into the PostgreSQL table. Finally, the script commits the changes to the PostgreSQL database and closes the connection.
