import pandas as pd
import os
import re
from datetime import datetime
from airflow import AirflowException
from airflow.models import Variable
from pos.modules.common import get_file_name, get_file_path
from pos.modules import db

def extract_date_from_report_name(folder_path):
    """
    Extract the date from the report name.
    The report name follows the 'pos nomenclature rules' which include only one date (see docs).

    :param folder_path: A string indicating the folder that contains the report.

    :return: A date.
    """
    file_name = get_file_name(folder_path=folder_path)
    match = re.search('\d{4}-\d{2}-\d{2}', file_name)
    week_starting_date = datetime.strptime(match.group(), '%Y-%m-%d').date()

    return week_starting_date


def extract_banner_from_report_name(folder_path):
    """
    Extract the banner name from the report name.
    The report name follows the 'pos nomenclature rules' which include the banner after the first '_' (see docs).

    :param folder_path: A string indicating the folder that contains the report.

    :return: A string.
    """
    file_name = get_file_name(folder_path=folder_path)
    banner = file_name.split('_', 2)[1]

    return banner


def generate_lookup_table(sql_query_file_path, connection_string, key_column, value_column):
    """
    Creates a 2 column data frame from a .sql file referring to two column indicating the item external number and genacol item number.
    Convert the data frame into a dictionary.

    :param sql_query_file_path: A string indicating the path to the .sql file.
    :param connection_string: A string indicating the connection information to a database.
        Follow this structure https://docs.sqlalchemy.org/en/13/core/engines.html.
        Recommended to pull connection string from Airflow connections.
    :param key_column: A string indicating a pandas column name that will be the dictionary key (CUSTITEMNMBR).
    :param value_column: A string indicating a pandas column name that will be the dictionary value (ITEMNMBR).

    :return: A dictionary of strings.
    """
    df = db.get_pandas_df_from_sql(sql_query_file_path=sql_query_file_path, connection_string=connection_string)
    df[df.columns] = df.apply(lambda x: x.str.strip())  # Trim white space in case the SQL type is char() instead of nvarchar().
    lookup_table = df.set_index(key_column)[value_column].to_dict()

    return lookup_table


def check_for_new_items(data_frame, column_name_to_check, lookup_table):
    """
    Check if each unique item in a data frame column exist in a lookup table.

    :param data_frame: A pandas data frame object.
    :param column_name_to_check: A pandas data frame column indicating the items to check (CUSTITEMNMBR)
    :param lookup_table: A dictionary of strings with the external item number as the key and Genacol item number as the value.

    :return: Boolean.
    """
    unique_items = data_frame[column_name_to_check].unique()

    new_items = []
    for item in unique_items:
        if item not in lookup_table.keys():
            new_items.append(item)

    if len(new_items) > 0:
        raise AirflowException('New item in report: %s' % str(new_items))
    else:
        return True


def add_genacol_item_number_column(data_frame, connection_string, sql_query_file_path, column_to_check='CUSTITEMNMBR',
                                   key_column='CUSTITEMNMBR',
                                   value_column='ITEMNMBR', new_column_name='ref_itemnmbr'):
    """
    Create a column indicating the genacol item number based on a lookup table.
    The parameters are fixed because they represent the SQL table columns and these are immutable.

    :param data_frame: A pandas data frame object.
    :param column_to_check: A string indicating the data frame column containing the external item number (see check_for_new_items function).
    :param connection_string: A string indicating the connection information to a database.
        Follow this structure https://docs.sqlalchemy.org/en/13/core/engines.html.
        Recommended to pull connection string from Airflow connections.
    :param sql_query_file_path: A string indicating the path to the .sql file used to generate the lookup table (see generate_lookup_table function).
    :param key_column: A string indicating a pandas column name that will be the dictionary key (see generate_lookup_table function).
    :param value_column: A string indicating a pandas column name that will be the dictionary value (see generate_lookup_table function).
    :param new_column_name: A string that will represent the new column name in the data frame.

    :return: A column in a data frame with the Genacol item number.
    """
    lookup_table = generate_lookup_table(sql_query_file_path=sql_query_file_path, connection_string=connection_string,
                                         key_column=key_column, value_column=value_column)

    check_for_new_items(data_frame=data_frame, column_name_to_check=column_to_check, lookup_table=lookup_table)

    data_frame[new_column_name] = data_frame[column_to_check].map(lookup_table)


def load_wrapper(func):
    """
    This function is a wrapper that insert a pandas df into a database after a certain function.

    :param func: A function that must return a data frame, a string indicating the name of the table to insert the data
        and a connection string (string) indicating the connection information to a database.
        Follow this structure https://docs.sqlalchemy.org/en/13/core/engines.html.
        Recommended to pull connection string from Airflow connections.

    :return: A function.
    """

    def f(*args, **kwargs):
        data_frame, table_name, connection_string = func(*args, **kwargs)
        db.insert_pandas_df_to_sql(data_frame=data_frame, table_name=table_name, connection_string=connection_string)

    return f


@load_wrapper
def wal_transform_load(folder_path, sql_query_file_path, table_name, connection_string):
    """
    Restructure the wal report to be compliant with the database table.
    Here are the mains steps:
    1. Select the column of interest.
    2. Rename the columns.
    3. Remove the rows with a quantity equal to 0.
    4. Generate a column with the date (see extract_date_from_report_name function).
    5. Generate the genacol item number (see add_genacol_item_number_column function).
    6. Drop the external item number column.
    7. Generate a column with the banner name (see extract_banner_from_report_name function).
    8. Reorganize the column order based on the sql table order (see pos_table_column_name_order in Airflow config variables).
    9. Change the format of certain column.

    :param folder_path: A string indicating the folder that contains the report.
    :param sql_query_file_path: A string indicating the path to the .sql file used to generate the lookup table (see add_genacol_item_number_column function).
    :param table_name: A string indicating the name of the SQL table that should received the data frame.
    :param connection_string:A string indicating the connection information to a database.
        Follow this structure https://docs.sqlalchemy.org/en/13/core/engines.html.
        Recommended to pull connection string from Airflow connections.

    :return: A pandas data frame compliant with the database format.
    """
    file_path = get_file_path(folder_path)
    df = pd.read_csv(file_path, header=None, sep='\t',
                     dtype={5: object})  # Force string on column 5 because values starts with '00'.
    df = df[[0, 1, 2, 3, 4, 5, 7]]
    df.columns = ['store_id', 'store_address', 'store_city', 'store_province', 'store_postalCode', 'CUSTITEMNMBR',
                  'pos_qty']
    df = df[df['pos_qty'] != 0]
    df['week_starting'] = extract_date_from_report_name(folder_path)
    add_genacol_item_number_column(data_frame=df, connection_string=connection_string,
                                   sql_query_file_path=sql_query_file_path)
    df = df.drop(columns=['CUSTITEMNMBR'])
    df['ref_custnmbr'] = 'WALM0000'
    df = df[Variable.get(key='databases', deserialize_json=True)['asterix_test']['pos_table_column_name_order']]
    df['store_id'] = df['store_id'].apply(int).apply(str)
    df['pos_qty'] = df['pos_qty'].apply(int)

    return df, table_name, connection_string


@load_wrapper
def uni_transform_load(folder_path, sql_query_file_path, table_name, connection_string):
    """
    Restructure the uni report to be compliant with the database table.
    Here are the mains steps:
    1. Select the column of interest.
    2. Remove the unnecessary header rows and the last row (refer to the total).
    3. Rename the columns.
    4. Generate a column with the date (see extract_date_from_report_name function).
    5. Generate the genacol item number (see add_genacol_item_number_column function).
    6. Drop the external item number column.
    7. Generate a column with the banner name (see extract_banner_from_report_name function).
    8. Generate the missing column and assign 'NULL'.
    9. Reorganize the column order based on the sql table order (see pos_table_column_name_order in Airflow config variables).

    :param folder_path: A string indicating the folder that contains the report.
    :param sql_query_file_path: A string indicating the path to the .sql file used to generate the lookup table (see add_genacol_item_number_column function).
    :param table_name: A string indicating the name of the SQL table that should received the data frame.
    :param connection_string: A string indicating the connection information to a database.
        Follow this structure https://docs.sqlalchemy.org/en/13/core/engines.html.
        Recommended to pull connection string from Airflow connections.

    :return: A pandas data frame compliant with the database format.
    """
    file_path = get_file_path(folder_path)
    df = pd.read_excel(file_path, header=None)
    df = df[[0, 13]]
    df = df.drop([0, 1, 2])
    df.drop(df.tail(1).index, inplace=True)  # Could also do:  df = df[df[0] != 'Total']
    df.columns = ['CUSTITEMNMBR', 'pos_qty']
    df['CUSTITEMNMBR'] = df['CUSTITEMNMBR'].str[:12]
    df['week_starting'] = extract_date_from_report_name(folder_path)
    add_genacol_item_number_column(data_frame=df, connection_string=connection_string,
                                   sql_query_file_path=sql_query_file_path)
    df = df.drop(columns=['CUSTITEMNMBR'])
    df['ref_custnmbr'] = 'UNIP0000'
    df['store_city'] = 'NULL'
    df['store_province'] = 'QC'
    df['store_id'] = 'NULL'
    df['store_address'] = 'NULL'
    df['store_postalCode'] = 'NULL'
    df = df[Variable.get(key='databases', deserialize_json=True)['asterix_test']['pos_table_column_name_order']]

    return df, table_name, connection_string


@load_wrapper
def jpc_transform_load(folder_path, sql_query_file_path, table_name, connection_string):
    """
    Restructure the jpc report to be compliant with the database table.
    Here are the mains steps:
    1. Select the column of interest.
    2. Rename the columns.
    3. Remove the rows with a quantity equal to 0.
    4. Generate a column with the date (see extract_date_from_report_name function).
    5. Change the external item number format to string to fit with the lookup table.
    6. Generate the genacol item number (see add_genacol_item_number_column function).
    7. Drop the external item number column.
    8. Generate a column with the banner name (see extract_banner_from_report_name function).
    9. Split the store city to remove the '(' and only obtain the city name.
    10. Reorganize the column order based on the sql table order (see pos_table_column_name_order in Airflow config variables).

    :param folder_path: A string indicating the folder that contains the report.
    :param sql_query_file_path: A string indicating the path to the .sql file used to generate the lookup table (see add_genacol_item_number_column function).
    :param table_name: A string indicating the name of the SQL table that should received the data frame.
    :param connection_string: A string indicating the connection information to a database.
        Follow this structure https://docs.sqlalchemy.org/en/13/core/engines.html.
        Recommended to pull connection string from Airflow connections.

    :return: A pandas data frame compliant with the database format.
    """
    file_path = get_file_path(folder_path)
    df = pd.read_csv(file_path, header=None, encoding='latin1', sep=';')
    df = df[[0, 2, 5, 6, 7, 11, 12]]
    df.columns = ['store_id', 'store_address', 'store_city', 'store_postalCode', 'store_province', 'CUSTITEMNMBR',
                  'pos_qty']
    df = df[df['pos_qty'] != 0]
    df['week_starting'] = extract_date_from_report_name(folder_path)
    df['CUSTITEMNMBR'] = df['CUSTITEMNMBR'].astype('str')
    add_genacol_item_number_column(data_frame=df, connection_string=connection_string,
                                   sql_query_file_path=sql_query_file_path)
    df = df.drop(columns=['CUSTITEMNMBR'])
    df['ref_custnmbr'] = "JEAN0000"
    df['store_city'] = df['store_city'].str.split('(').str[0]
    df = df[Variable.get(key='databases', deserialize_json=True)['asterix_test']['pos_table_column_name_order']]

    return df, table_name, connection_string


def shp_stores_transform(folder_path):
    """
    Restructure the shp stores report in order to join it eventually with the shp report.

    :param folder_path: A string indicating where the file to transform reside.
        The structure ensure that only one file at a time is present in the folder.
        This is why we can assume that the first file is the right one.

    :return: A pandas data frame object.
    """
    file_path = get_file_path(folder_path)
    df = pd.read_excel(file_path, header=0, dtype={'Site #': object})
    df = df.iloc[:, [1, 7, 8, 9, 10]]
    df.columns = ['store_id', 'store_address', 'store_city', 'store_province', 'store_postalCode']
    df['store_province'] = df['store_province'].replace('PQ', 'QC')

    return df


def init_shp_stores(save_to_folder, update_folder_path):
    """
    Takes a shp stores report from the update_folder_path, restructures it (see shp_stores_transform function)
    and saves it in the save_to_folder. Finally, it deletes the original shp stores report.
    The created report becomes the master file.

    :param save_to_folder: A string indicating to the folder that should receive the final report.
    :param update_folder_path: A string indicating the folder that contains the report to transform.

    :return: A new master file in save_to_folder and no more file in update_folder_path.
    """
    file_path = get_file_path(update_folder_path)
    df = shp_stores_transform(folder_path=update_folder_path)
    df.to_csv(save_to_folder + '/' + 'shp_master_store_list.csv', index=False)
    os.remove(file_path)


def update_shp_stores(master_folder_path, update_folder_path):
    """
    Takes a shp stores report from the update_folder_path, restructures it (see shp_stores_transform function).
    Then the master file (see dags/pos/data/shp/master) is updated with the new 'store_id'.
    If rows are updated, the master file is updated and the one used to update is deleted.
    Else, the file used to update is deleted.

    :param update_folder_path: A string indicating the folder that contains the report use to update.

    :return: Update the master file and delete the file used for the update.
    """
    if len(os.listdir(master_folder_path)) == 0:
        init_shp_stores(save_to_folder=master_folder_path, update_folder_path=update_folder_path)
    else:
        master = pd.read_csv(master_folder_path + '/shp_master_store_list.csv', dtype={'store_id': object})
        new = shp_stores_transform(folder_path=update_folder_path)

        # Add new rows
        left_join_on_new = pd.merge(new, master['store_id'], on='store_id', how='left', indicator=True)
        only_in_new = left_join_on_new[left_join_on_new['_merge'] == 'left_only'].drop(columns=['_merge'])
        updated_master = master.append(only_in_new)
        print("DEBUG: Number of rows added = " + str(len(updated_master) - len(master)))

        # Update Values
        updated_master.set_index('store_id', inplace=True)
        updated_master.update(new.set_index('store_id'))
        updated_master = updated_master.reset_index()

        updated_master.to_csv(master_folder_path + '/shp_master_store_list.csv', index=False)
        os.remove(update_folder_path + '/' + get_file_name(update_folder_path))


@load_wrapper
def shp_transform_load(folder_path, sql_query_file_path, table_name, connection_string, store_master_file_path):
    """
    Restructure the shp report to be compliant with the database table.
    Here are the mains steps:
    1. Select the column of interest.
    2. Rename the columns.
    3. Removes the rows where '-' is in the quantity column (refers to a null quantity).
    4. Generate a column with the date (see extract_date_from_report_name function).
    5. Change the external item number format to string to fit with the lookup table.
    6. Import the shp_stores.csv file (if not exist see init_shp_stores function).
    7. Merge the store ids from the report and the store file to check if new store id are present in the report.
        If so, the process stops and it recommends the user to manually call the 'update_shp_stores' function.
    8. Generate the genacol item number (see add_genacol_item_number_column function).
    9. Drop the external item number column.
    10. Generate a column with the banner name (see extract_banner_from_report_name function).
    11. Reorganize the column order based on the sql table order (see pos_table_column_name_order in Airflow config variables).

    :param folder_path: A string indicating the folder that contains the report.
    :param sql_query_file_path: A string indicating the path to the .sql file used to generate the lookup table (see add_genacol_item_number_column function).
    :param table_name: A string indicating the name of the SQL table that should received the data frame.
    :param connection_string: A string indicating the connection information to a database.
        Follow this structure https://docs.sqlalchemy.org/en/13/core/engines.html.
        Recommended to pull connection string from Airflow connections.

    :return: A pandas data frame compliant with the database format.
    """
    file_path = get_file_path(folder_path)
    df = pd.read_excel(file_path, header=0,
                       dtype={'Site #': object})  # Force string on column because values starts with '0'.
    df = df.iloc[:, [1, 3, (len(df.columns) - 1)]]
    df.columns = ['store_id', 'CUSTITEMNMBR', 'pos_qty']
    df = df[df['pos_qty'] != '-']
    df['week_starting'] = extract_date_from_report_name(folder_path)
    df['CUSTITEMNMBR'] = df['CUSTITEMNMBR'].astype('str')
    store_infos = pd.read_csv(store_master_file_path, dtype={'store_id': object})

    # Double verification for the store
    df_merged = df.merge(store_infos, how='left', on='store_id', indicator=True)
    left_only = df_merged[df_merged['_merge'] == 'left_only']

    if len(left_only) != 0:
        raise AirflowException(
            "These Store ID in report dont find a match in the master file: %s." % left_only.values.tolist())
    else:
        df = df_merged.drop(columns=['_merge'])

    add_genacol_item_number_column(data_frame=df, connection_string=connection_string,
                                   sql_query_file_path=sql_query_file_path)
    df = df.drop(columns=['CUSTITEMNMBR'])
    df['ref_custnmbr'] = 'SHOP0000'
    df = df[Variable.get(key='databases', deserialize_json=True)['asterix_test']['pos_table_column_name_order']]

    return df, table_name, connection_string


@load_wrapper
def cos_transform_load(folder_path, sql_query_file_path, table_name, connection_string):
    file_path = get_file_path(folder_path)
    df = pd.read_csv(file_path, header=1, delimiter=';', dtype={'Item Number': float, 'Warehouse Code': object})
    df = df.drop(columns=['Item', 'Venue'])
    df = df.dropna(axis=0, thresh=6)
    df.columns = [
        "CUSTITEMNMBR",
        "store_city",
        "store_province",
        "store_id",
        "store_address",
        "store_postalCode",
        "pos_qty"]
    df = df.dropna(axis=0, subset=['pos_qty'])
    df = df[df['pos_qty'] != 0]
    df['pos_qty'] = df['pos_qty'].astype(int)
    df['week_starting'] = extract_date_from_report_name(folder_path)
    df['ref_custnmbr'] = 'COST0000'
    df['CUSTITEMNMBR'] = df['CUSTITEMNMBR'].astype('int')
    df['CUSTITEMNMBR'] = df['CUSTITEMNMBR'].astype('str')
    add_genacol_item_number_column(data_frame=df, connection_string=connection_string,
                                   sql_query_file_path=sql_query_file_path)
    df = df.drop(columns=['CUSTITEMNMBR'])
    df = df[Variable.get(key='databases', deserialize_json=True)['asterix_test']['pos_table_column_name_order']]

    return df, table_name, connection_string

