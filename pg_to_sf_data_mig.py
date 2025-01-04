import csv
import psycopg2
import snowflake.connector
# Connect to PostgreSQL
pg_conn = psycopg2.connect(
    host='localhost',
    port='5432',
    user='postgres',
    password='pgadmin',
    database='postgres'
)
pg_cursor = pg_conn.cursor()
# Export data from PostgreSQL to CSV
# Execute a SELECT query to fetch data
pg_cursor.execute("SELECT s_id, s_name FROM sf_data")
# Fetch all rows
rows = pg_cursor.fetchall()
# Write data to the CSV file
with open('F:/postgresql_training1_ati/pg_sf_data.csv', 'w', newline='') as csv_file:
    csv_writer = csv.writer(csv_file, delimiter='|')
    # Write header
    csv_writer.writerow(['s_id', 's_name'])
    # Write data rows
    csv_writer.writerows(rows)
# Connect to Snowflake
snowflake_conn = snowflake.connector.connect(
    user='cn03',
    password='Doluv2bluvd',
    account='kbefsvr-tm08737',
    warehouse="COMPUTE_WH",
    database='ATI_TRAINING_DB',
    schema='ATI_ST_SCHEMA'
)
snowflake_cursor = snowflake_conn.cursor()
# Remove existing content from the internal stage
remove_command = "REMOVE @~/pg_sf_stage_py;"
snowflake_cursor.execute(remove_command)
# Upload to Snowflake internal stage
upload_command = f"PUT file://F:/postgresql_training1_ati/pg_sf_data.csv @~/pg_sf_stage_py"
snowflake_cursor.execute(upload_command)
# Create Snowflake table
create_table = "CREATE or replace TABLE sf_py_data (s_id integer, s_name text);"
snowflake_cursor.execute(create_table)
# Copy data from internal stage to Snowflake table
copy_command = f"COPY INTO sf_py_data FROM @~/pg_sf_stage_py/pg_sf_data.csv FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"')"
snowflake_cursor.execute(copy_command)
# Close connections
pg_cursor.close()
pg_conn.close()
snowflake_cursor.close()
snowflake_conn.close()
