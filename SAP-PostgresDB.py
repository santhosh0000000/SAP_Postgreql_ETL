import pyhdb
import psycopg2

# SAP HANA Connection Parameters
hana_host = '192.168.77.162'
hana_port = 36215
hana_user = 'MGPUSER'
hana_password = ')(&*(^#GJHGNJV))'
hana_schema = 'SMPD'
hana_table = 'MDP'  # Replace with the actual table name

# PostgreSQL Connection Parameters
postgres_host = '192.168.247.13'
postgres_port = '5432'
postgres_user = 'postgres'
postgres_password = 'postgres'
postgres_db = 'SMPD'
postgres_table = 'MDP'  # Replace with the desired table name

# Connect to SAP HANA database
hana_conn = pyhdb.connect(
    host=hana_host,
    port=hana_port,
    user=hana_user,
    password=hana_password
)

# Create a cursor for SAP HANA database
hana_cursor = hana_conn.cursor()

# Execute SQL query to retrieve data from SAP HANA table
hana_cursor.execute(f'SELECT * FROM "{hana_schema}"."{hana_table}"')

# Fetch column names from SAP HANA result set
column_names = [desc[0] for desc in hana_cursor.description]

# Fetch all rows from the SAP HANA result set
rows = hana_cursor.fetchall()

# Close the cursor and SAP HANA connection
hana_cursor.close()
hana_conn.close()

# Connect to PostgreSQL database
postgres_conn = psycopg2.connect(
    host=postgres_host,
    port=postgres_port,
    dbname=postgres_db,
    user=postgres_user,
    password=postgres_password
)

# Create a cursor for PostgreSQL database
postgres_cursor = postgres_conn.cursor()

# Generate the CREATE TABLE statement for PostgreSQL
create_table_sql = f'CREATE TABLE "{postgres_table}" ('

for column_name in column_names:
    create_table_sql += f'\n"{column_name}" TEXT,'

# Remove the trailing comma and close the CREATE TABLE statement
create_table_sql = create_table_sql.rstrip(',') + '\n);'

# Execute the CREATE TABLE statement in PostgreSQL
postgres_cursor.execute(create_table_sql)

# Generate the INSERT INTO statement for PostgreSQL
column_names_quoted = ['"' + col + '"' for col in column_names]
placeholders = ', '.join(['%s'] * len(column_names))
insert_query = f'INSERT INTO "{postgres_table}" ({", ".join(column_names_quoted)}) VALUES ({placeholders})'

# Insert data into PostgreSQL using batch inserts
postgres_cursor.executemany(insert_query, rows)

# Commit the changes to PostgreSQL
postgres_conn.commit()

# Close the cursor and PostgreSQL connection
postgres_cursor.close()
postgres_conn.close()