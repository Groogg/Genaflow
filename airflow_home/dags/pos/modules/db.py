from sqlalchemy import create_engine
import pandas as pd


def get_pandas_df_from_sql(sql_query_file_path, connection_string):
    """
    Read a .sql file and return the result in a pandas data frame.

    :param sql_query_file_path: A string indicating the path to the .sql file.
    :param connection_string: A string indicating the connection information to a database.
        Follow this structure https://docs.sqlalchemy.org/en/13/core/engines.html.
        Recommended to pull connection string from Airflow connections.

    :return: A pandas data frame object containing the query result.
    """
    query = open(sql_query_file_path, 'r').read()

    engine = create_engine(connection_string)

    return pd.read_sql_query(query, engine)


def insert_pandas_df_to_sql(data_frame, table_name, connection_string):
    """
    Insert pandas data frame in database table with the rollback function.

    :param data_frame: Pandas data frame object.
    :param table_name: A string indication the SQL table name.
    :param connection_string: A string indicating the connection information to a database.
        Follow this structure https://docs.sqlalchemy.org/en/13/core/engines.html.
        Recommended to pull connection string from Airflow connections.

    :return: Insert rows into a database table.
    """
    engine = create_engine(connection_string)

    with engine.begin() as conn:
        data_frame.to_sql(table_name, con=conn, index=False, if_exists='append')

    print("Record inserted successfully into %s table" % table_name)

