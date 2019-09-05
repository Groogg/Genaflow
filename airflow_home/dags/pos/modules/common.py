import json
import os
import time
from airflow.models import AirflowException


def move_file(from_folder_path, to_folder_path, prefix):
    """
    Move the only file in the from_path to the to_folder_path.
    The structure ensure that only one file at a time is present in the folder.
    This is why we can assume that the first file is the right one.

    :param from_folder_path: A string indicating the path where the file is at the moment.
    :param to_folder_path: A string indicating the path where the file will be move.
    :param prefix: A string that will be place before the report name in the destination folder.

    :return: Move a file from one folder to another.
    """
    if prefix == None:
        os.rename(get_file_path(from_folder_path), to_folder_path + '/' + get_file_name(from_folder_path))
    else:
        os.rename(get_file_path(from_folder_path), to_folder_path + '/%s_' % str(prefix) + get_file_name(from_folder_path))


def read_json(file_path):
    with open(file_path) as json_file:
        return json.load(json_file)


def folder_count_sensor(folder_path, wait, nbr_of_files, timeout_delay=120):
    """
    Check if a folder contains a certain number of files.
    If wait = True, then it keep checking until the timeout_delay is reached.
    Otherwise, it return False.

    :param folder_path: A string representing the path of the folder to check.
    :param wait: A boolean indicating if it should wait for files to appear.
    :param nbr_of_files: A integer of the number of files to check for.
    :param timeout_delay: A integer of the time to wait in seconds.

    :return: Boolean
    """
    sleep_counter = 0
    if len(os.listdir(folder_path)) == nbr_of_files:
        return True
    else:
        if wait == True:
            while len(os.listdir(folder_path)) != nbr_of_files:
                if sleep_counter >= timeout_delay:
                    raise AirflowException("Timeout.")
                else:
                    sleep_counter += 2
                    print("Waiting 2 seconds before looking again.")
                    time.sleep(2)
            return True

        raise AirflowException("Incorrect file count.")


def get_file_name(folder_path, all_files=False):
    """
    Return a string representing the file name of the first file in folder.
    The structure ensure that only one file at a time is present in the folder.
    This is why we can assume that the first file is the right one.
    """
    return os.listdir(folder_path)[0]


def get_file_extension(file_name):
    """
    Return a string representing the file extension of the first file in folder.
    """
    return os.path.splitext(file_name)[1].lower()


def get_file_path(folder_path):
    """
    Return a string representing the file path of the first file in folder.
    The structure ensure that only one file at a time is present in the folder.
    This is why we can assume that the first file is the right one.
    """
    return folder_path + '/' + get_file_name(folder_path)
