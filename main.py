import pyodbc as db
import pandas as pd
import cx_Oracle
from src import queries
from src.queries import select_main_table, select_mirror_table
from src.verify_data_function import verify_and_add_column

# Connection to Database
try:
    conn_sql = db.connect(
        driver="SQL Server",
        server="DESKTOP-NICO",
        database="compare_data"  # Name DB SQL Server
    )
    print("Successful connection to SQL Server!")
except db.Error as ex:
    print("Error Connecting to SQL Server!", ex)

try:
    # Create mirror table in SQL Server if it doesn't exist
    cursor_sql = conn_sql.cursor()
    try:
        cursor_sql.execute(queries.create_mirror_table)
        cursor_sql.commit()
        print("Mirror_table created in SQL Server.")
    except db.Error:
        print("Mirror Table Exists!")

    ############ Oracle Logic ############

    # Connection to Oracle and data reading
    dsn_tns = cx_Oracle.makedsn('localhost', '1521', service_name='XE')
    conn_oracle = cx_Oracle.connect(user='nico', password='root', dsn=dsn_tns)
    print("Successful connection to Oracle!")

    cursor_oracle = conn_oracle.cursor()
    cursor_oracle.execute(queries.query_oracle)

    # Fetch all results from Oracle
    rows_oracle = cursor_oracle.fetchall()

    # Convert results into a Pandas DataFrame
    df_oracle = pd.DataFrame(rows_oracle, columns=[desc[0] for desc in cursor_oracle.description])

    if not df_oracle.empty:
        # Transfer data to SQL Server dynamically
        columns = list(df_oracle.columns)
        placeholders = ','.join(['?'] * len(columns))
        insert_query = f"INSERT INTO mirror_table ({','.join(columns)}) VALUES ({placeholders})"

        # Check if rows already exist in mirror_table
        existing_ids_query = "SELECT ID FROM mirror_table"
        existing_ids = pd.read_sql(existing_ids_query, conn_sql)['ID'].tolist()

        # Filter out rows that already exist
        df_to_insert = df_oracle[~df_oracle['ID'].isin(existing_ids)]

        if not df_to_insert.empty:
            # Execute the insertion in SQL Server
            cursor_sql.fast_executemany = True
            cursor_sql.executemany(insert_query, df_to_insert.values.tolist())
            conn_sql.commit()

            print(f"{len(df_to_insert)} rows have been transferred from Oracle to SQL Server.")
        else:
            print("All rows already exist in mirror_table. No new rows added.")
    else:
        print("No rows found in ORACLE_TABLE in Oracle.")

    ############ SQL Server Logic ############

    # Compare if there are new data
    verify_and_add_column(conn_sql, 'mirror_table', 'isNew_element', 'VARCHAR(50)')

    main_table = pd.read_sql(select_main_table, conn_sql)
    mirror_table = pd.read_sql(select_mirror_table, conn_sql)

    # Compare
    mirror_table['isNew_element'] = mirror_table['ID'].apply(
        lambda x: 'YES' if x not in main_table['ID'].values else ''
    )

    # Save Changes
    cursor = conn_sql.cursor()
    # Update Table
    for index, row in mirror_table.iterrows():
        update_query = """
                UPDATE mirror_table
                SET isNew_element = ?
                WHERE id = ?
                """
        cursor.execute(update_query, row['isNew_element'], row['ID'])

    conn_sql.commit()
    print("Compare and update successful.")
except Exception as ex:
    print("Error executing:", ex)
finally:
    # Close connections
    if 'conn_sql' in locals():
        conn_sql.close()
    if 'conn_oracle' in locals():
        conn_oracle.close()
