# Function to check and add the column if it does not exist
def verify_and_add_column(conn, table_name, column_name, column_type):
    cursor = conn.cursor()
    check_column_query = f"""
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{table_name}' AND COLUMN_NAME = '{column_name}'
    """
    cursor.execute(check_column_query)
    result = cursor.fetchone()

    if not result:
        add_column_query = f"""
        ALTER TABLE {table_name}
        ADD {column_name} {column_type}
        """
        cursor.execute(add_column_query)
        conn.commit()
        print(f"Column Add! '{column_name}' a '{table_name}'.")