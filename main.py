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
    print("Succesful connection to SQL Server!")
except db.Error as ex:
    print("Error Connecting to SQL Server!", ex)

try:
    # Crear tabla espejo en SQL Server si no existe
    cursor_sql = conn_sql.cursor()
    try:
        cursor_sql.execute(queries.create_mirror_table)
        cursor_sql.commit()
        print("Tabla mirror_table creada en SQL Server.")
    except db.Error:
        print("Mirror Table Exist!")

    ############ Oracle Logic ############

    # Conexión a Oracle y lectura de datos
    dsn_tns = cx_Oracle.makedsn('localhost', '1521', service_name='XE')
    conn_oracle = cx_Oracle.connect(user='nico', password='root', dsn=dsn_tns)
    print("Conexión exitosa a Oracle!")

    cursor_oracle = conn_oracle.cursor()
    cursor_oracle.execute(queries.query_oracle)

    # Obtener todos los resultados de Oracle
    rows_oracle = cursor_oracle.fetchall()

    # Convertir los resultados en un DataFrame de Pandas
    df_oracle = pd.DataFrame(rows_oracle, columns=[desc[0] for desc in cursor_oracle.description])

    if not df_oracle.empty:
        # Transferir datos a SQL Server de manera dinámica
        columns = list(df_oracle.columns)
        placeholders = ','.join(['?'] * len(columns))
        insert_query = f"INSERT INTO mirror_table ({','.join(columns)}) VALUES ({placeholders})"

        # Ejecutar la inserción en SQL Server
        cursor_sql.fast_executemany = True
        cursor_sql.executemany(insert_query, df_oracle.values.tolist())
        conn_sql.commit()

        print(f"Se han transferido {len(df_oracle)} filas desde Oracle a SQL Server.")
    else:
        print("No se encontraron filas en la tabla ORACLE_TABLE en Oracle.")

    ############ SQL Server Logic ############

    #Compare if exist new data
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
    print("Compare and update successfully.")
except Exception as ex:
    print("Error to execute:", ex)
finally:
    # Cerrar conexiones
    if 'conn_sql' in locals():
        conn_sql.close()
    if 'conn_oracle' in locals():
        conn_oracle.close()