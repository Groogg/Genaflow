from airflow import AirflowException
from airflow.models import Variable
from xlrd import open_workbook
from zipfile import ZipFile
import os
from pos.modules.common import get_file_name, get_file_extension, folder_count_sensor


def generate_report_id(folder_path, id_type, nbr_rows=5):
    """
    Generate an id for a report.
    If id_type = name the report name will be the id.
    If the id_type = rows the first N rows will be the id.

    :param folder_path: A string representing the path of the folder to check.
    :param id_type: A string, either equal to 'rows' or 'name'.
    :param nbr_rows: A integer of the number of rows to use for the id.

    :return: A string representing an id.
    """
    file_name = get_file_name(folder_path=folder_path)
    file_extension = get_file_extension(file_name=file_name)

    if id_type == "name":
        return file_name

    elif id_type == "rows":
        if file_extension in [".xls", ".xlsx"]:
            excel_sheet = open_workbook(folder_path + '/' + file_name).sheet_by_index(0)
            return excel_sheet.row_values(0)
        else:
            with open(folder_path + '/' + file_name, encoding="latin-1") as file:
                return [next(file) for x in range(nbr_rows)]
    else:
        raise AirflowException('Invalid id_type.')


def check_if_new_report(old_id, folder_path, id_type):
    """
    Compare the old and new report_id.
    If different, update the 'pos_dump_report_id' Airflow variable.

    :param old_id: Any type, preferably the same as the new id to avoid error.

    :return: Update the 'pos_dump_report_id' Airflow variable.
    """
    new_id = generate_report_id(folder_path, id_type)

    if new_id == old_id:
        raise AirflowException("The report id is the same as the old one.")
    else:
        Variable.set(key='pos_dump_report_id', value=new_id)


def unzip(file_path):
    """
    Extract a file from a zip into the same directory and delete the zip.

    :param file_path: A string representing the path to the zip file.

    :return: A string representing the file name inside the zip.
    """
    folder_path = os.path.dirname(file_path)
    in_zip_file_list = ZipFile(file_path).namelist()

    if len(in_zip_file_list) != 1:
        raise AirflowException('Zip file contains more than one file.')
    else:
        in_zip_file_name = in_zip_file_list[0]
        with ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(folder_path)
        folder_count_sensor(folder_path=folder_path, wait=True, nbr_of_files=2)
        os.remove(file_path)

        return in_zip_file_name


def rename_report(folder_path, banner, week_starting_date):
    """
    Rename the report based on the pos nomenclature rules (see docs).

    :param folder_path: A string path of the folder to check.
    :param banner: A string representing the banner acronym.
    :param week_starting_date: A string representing a date.

    :return: Rename the file in the folder.
    """
    file_name = get_file_name(folder_path=folder_path)
    file_extension = get_file_extension(file_name=file_name)

    if file_extension == '.zip':
        file_name = unzip(file_path=folder_path + '/' + file_name)
        file_extension = get_file_extension(file_name)

    os.rename(folder_path + '/' + file_name,
              '%s/pos_%s_%s%s' % (folder_path, banner, week_starting_date, file_extension))
