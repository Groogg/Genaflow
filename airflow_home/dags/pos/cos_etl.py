from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from pos.modules import common, extract, selenium, transform_load, report_id_tracker
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

# Limits the number of query to the airflow db by creating global variable
BANNER = 'cos'
PATHS = Variable.get('paths', deserialize_json=True)
CONFIG = Variable.get(BANNER, deserialize_json=True)
DATABASE = Variable.get('databases', deserialize_json=True)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 8, 24),
    'email': ['gregg.gilbert@genacol.ca', 'gregggilbert16@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True
}

dag = DAG('%s_etl' % BANNER, default_args=default_args, schedule_interval=None, catchup=False)

with dag:
    check_one_report_in_folder = PythonOperator(
        task_id='check_one_report_in_folder',
        python_callable=extract.folder_count_sensor,
        op_kwargs={'folder_path': PATHS['download_report_folder'],
                   'wait': False,
                   'nbr_of_files': 1}
    )

    check_if_new_report = PythonOperator(
        task_id='check_if_new_report',
        python_callable=extract.check_if_new_report,
        op_kwargs={'old_id': common.read_json(PATHS['report_tracker_json_path'])[BANNER]['report_last_id'],
                   'folder_path': PATHS['download_report_folder'],
                   'id_type': CONFIG['report']['id_type']}
    )

    transform_load = PythonOperator(
        task_id='transform_load',
        python_callable=transform_load.cos_transform_load,
        op_kwargs={'folder_path': PATHS['download_report_folder'],
                   'sql_query_file_path': PATHS['sql'] + "/%s_customer_itemnmbr.sql" % BANNER,
                   'table_name': "POS",
                   'connection_string': MsSqlHook('asterix_test').get_uri() + DATABASE['drivers']['sql_server_17']}
    )

    archive_report = PythonOperator(
        task_id='archive_report',
        python_callable=common.move_file,
        op_kwargs={'from_folder_path': PATHS['download_report_folder'],
                   'to_folder_path': PATHS['report_archive'],
                   'prefix': None}
    )

    update_report_tracker = PythonOperator(
        task_id='update_report_tracker',
        python_callable=report_id_tracker.update_report_tracker,
        op_kwargs={'file_path': PATHS['report_tracker_json_path'],
                   'banner': BANNER,
                   'new_id': Variable.get(key='pos_dump_report_id')}
    )

    ONE_FAILED__move_report = PythonOperator(
        task_id='ONE_FAILED__move_report',
        python_callable=common.move_file,
        op_kwargs={'from_folder_path': PATHS['download_report_folder'],
                   'to_folder_path': PATHS['report_failed'],
                   'prefix': "failed"},
        trigger_rule=TriggerRule.ONE_FAILED
    )

    check_one_report_in_folder >> check_if_new_report >> transform_load >> archive_report \
    >> update_report_tracker >> ONE_FAILED__move_report