import mysql.connector
import numpy as np
import pandas as pd

def create_connection(db_config):
    """
    Establishes a connection to the MySQL database.
    """
    try:
        connection = mysql.connector.connect(**db_config)
        print('Connection established.')
        return connection
    except mysql.connector.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def prepare_data(df):
    """
    Prepares the DataFrame for database insertion by handling NaN values and converting types.
    """
    df = df.replace({np.nan: None})
    for col in df.select_dtypes(include=["float64", "int64"]).columns:
        df[col] = df[col].astype("Int64")  # Int64 handles None values
    return df.values.tolist()

def to_sql(df, table_name, column_names, db_config, batch_size=100):
    """
    Inserts a DataFrame into a MySQL table.
    """
    connection = create_connection(db_config)
    if not connection:
        return

    try:
        data = prepare_data(df)
        cursor = connection.cursor()
        placeholders = ", ".join(["%s"] * len(column_names.split(', ')))
        row_count = len(data)
        n = 0

        # Process data in batches
        for i in range(0, row_count, batch_size):
            batch = data[i:i + batch_size]
            try:
                cursor.executemany(
                    f"""
                    INSERT INTO {table_name}({column_names}) 
                    VALUES ({placeholders})
                    """,
                    batch
                )
                connection.commit()
            except:
                for row in batch:
                    try:
                        cursor.execute(
                            f"""
                            INSERT INTO {table_name}({column_names})
                            VALUES ({placeholders})
                            """,
                            row
                        )
                        connection.commit()
                    except mysql.connector.Error as e:
                        n += 1
                        print(f"Error inserting data: {e}")
                        print(f"Failed Row: {row}")
        print(f'{row_count - n} out of {row_count} rows inserted successfully into the table.')
        cursor.close()
    finally:
        connection.close()
        print('Connection closed.')