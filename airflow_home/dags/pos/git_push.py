from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

PROJECT_ROOT_DIR = "/home/genacol/projects/genaflow"
PATHS = Variable.get('paths', deserialize_json=True)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 8, 22),
    'email': ['gregg.gilbert@genacol.ca', 'gregggilbert16@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True
}

dag = DAG('git_push', default_args=default_args, schedule_interval='0 7 * * 5,6,0', catchup=False)

with dag:
    commit_and_push = BashOperator(
        task_id='commit_and_push',
        bash_command="cd /home/genacol/projects/genaflow && git add . && git commit -m 'Weekly Git Update' && git push origin develop"
    )
