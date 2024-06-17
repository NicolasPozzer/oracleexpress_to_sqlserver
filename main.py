import pyodbc as db
import pandas as pd
from src import queries, verify_data_function

# Connection to SQL Server
try:
    conn = db.connect(
        driver="SQL Server",
        server="DESKTOP-NICO",
        database="compare_data"#name DB SQL Server
    )
    print("Succesful connection!")
except db.Error as ex:
    print("Error connecting: ",ex)


try:
    # Create Mirror table SQL Server
    cursor = conn.cursor()
    cursor.execute(queries.create_mirror_table)
    cursor.commit()

    ############ Oracle Logic ############

    #Read data to oracle

    #Transfer data to SQL Server


    ############ SQL Server Logic ############

    #Compare if exist new data
    verify_data_function.verify_and_add_column(conn, 'main_table', 'isNew_element', 'VARCHAR(50)')
    main_table = pd.read_sql(queries.select_main_table, conn)
    mirror_table = pd.read_sql(queries.select_mirror_table, conn)

    # Compare
    main_table['isNew_element'] = main_table['ID'].apply(
        lambda x: 'YES' if x not in mirror_table['ID'].values else ''
    )

    # Save Changes
    cursor = conn.cursor()
    # Update Table
    for index, row in main_table.iterrows():
        update_query = """
                UPDATE main_table
                SET isNew_element = ?
                WHERE id = ?
                """
        cursor.execute(update_query, row['isNew_element'], row['ID'])

    conn.commit()
    print("Compare and update successfully.")

except db.Error as ex:
    print("Error Execute: ", ex)
finally:
    # Close the connection whether errors occur or not.
    if 'conn' in locals():
        conn.close()