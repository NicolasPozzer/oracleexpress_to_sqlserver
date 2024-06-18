"""
df_oracle = pd.read_sql(query_oracle, con=conn_oracle)

    # Transferir datos a SQL Server de manera dinámica
    cursor.fast_executemany = True

    # Obtener dinámicamente los nombres de las columnas
    columns = list(df_oracle.columns)
    placeholders = ','.join(['?'] * len(columns))
    insert_query = f"INSERT INTO mirror_table ({','.join(columns)}) VALUES ({placeholders})"

    # Execute Transfer data to SQL Server
    cursor.executemany(insert_query, df_oracle.values.tolist())
    conn.commit()

    print(f"Se han transferido {len(df_oracle)} filas desde Oracle a SQL Server.")



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
        update_query =
                UPDATE main_table
                SET isNew_element = ?
                WHERE id = ?

        cursor.execute(update_query, row['isNew_element'], row['ID'])

    conn.commit()
    print("Compare and update successfully.")"""
