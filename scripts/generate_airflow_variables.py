import json
import sys
import time
import os

# THESE VARIABLES ARE MEANT TO BE CHANGED
PROJECT_ROOT_DIR = "/home/genacol/projects/genaflow"
WEB_DRIVER_DIR = "/usr/bin/chromedriver"

data = {
    "jpc": {
        "online_portal": {
            "url": "https://ted.jeancoutu.com/action/login",
            "login_username": "PRIVATE",
            "login_password": "PRIVATE",
            "html_username_attribute": ":2",
            "html_password_attribute": ":3",
            "html_login_button_attribute": "login-bts"
        },
        "report": {
            "id_type": "name"
        }
    },
    "cos": {
        "online_portal": {
            "url": "https://advantage.iriworldwide.com/CRXRPM/SplashLogin.jsp",
            "login_username": "PRIVATE",
            "login_password": "PRIVATE",
            "html_username_attribute": "",
            "html_password_attribute": ""
        },
        "report": {
            "id_type": "rows"
        }
    },
    "uni": {
        "online_portal": {
            "url": "https://extranet.uniprix.net/index.php",
            "login_username": "PRIVATE",
            "login_password": "PRIVATE",
            "html_username_attribute": "Username",
            "html_password_attribute": "Password",
            "html_login_button_attribute": "btnSubmit"
        },
        "report": {
            "id_type": "name"
        }
    },
    "wal": {
        "online_portal": {
            "url": "https://retaillink.wal-mart.com",
            "login_username": "PRIVATE",
            "login_password": "PRIVATE",
            "html_username_attribute": "txtUser",
            "html_password_attribute": "txtPass",
            "html_login_button_attribute": "Login"
        },
        "report": {
            "id_type": "rows"
        }
    },
    "shp": {
        "online_portal": {
            "url": "https://shoppersdrugmart.precima.io/login",
            "login_username": "PRIVATE",
            "login_password": "PRIVATE",
            "html_username_attribute": "mat-input-0",
            "html_password_attribute": "passwordInput",
            "html_login_button_attribute": "signin-button mat-raised-button ng-star-inserted"
        },
        "report": {
            "id_type": "rows"
        }
    },
    "paths": {
        "download_report_folder": PROJECT_ROOT_DIR + "/airflow_home/dags/pos/data/report_download",
        "chrome_driver_path": WEB_DRIVER_DIR,
        "report_tracker_json_path": PROJECT_ROOT_DIR + "/airflow_home/dags/pos/data/report_tracker.json",
        "report_archive": PROJECT_ROOT_DIR + "/airflow_home/dags/pos/data/report_archive",
        "report_failed": PROJECT_ROOT_DIR + "/airflow_home/dags/pos/data/report_failed",
        "sql": PROJECT_ROOT_DIR + "/airflow_home/dags/pos/sql",
        "shp_master_folder": PROJECT_ROOT_DIR + "/airflow_home/dags/pos/data/shp_store/master",
        "shp_update_folder": PROJECT_ROOT_DIR + "/airflow_home/dags/pos/data/shp_store/update"
    },
    "pos_dump_report_id": "None",
    "databases": {
        "drivers": {
            "sql_server_17":  "?driver=ODBC+Driver+17+for+SQL+Server"
        },
        "asterix_test": {
            "pos_table_column_name_order": ["ref_itemnmbr", "store_city", "store_province", "store_id", "store_address",
                                            "store_postalCode", "ref_custnmbr", "pos_qty", "week_starting"]
        },
        "obelix_prod": {
            "pos_table_column_name_order": ["ref_itemnmbr", "store_city", "store_province", "store_id", "store_address",
                                            "store_postalCode", "ref_custnmbr", "pos_qty", "week_starting"]
        }
    },
    "email": {
        "airflow_alerts": ["gregg.gilbert@genacol.ca", "gregggilbert16@gmail.com", "caroline.baker@genacol.ca"]
    }
}


def to_json():
    with open(PROJECT_ROOT_DIR + '/.config/airflow_variables.json', 'w') as outfile:
        json.dump(data, outfile)

    print("Success: Json file created in .config.")


def to_airflow():
    os.system("cd /home/genacol/projects/genaflow/pyvenv/bin/ && . activate && airflow variables --import %s/.config/airflow_variables.json" % PROJECT_ROOT_DIR)


def generate_airflow_variables(export_type):
    if export_type == "airflow":
        to_json()
        time.sleep(3)  # Wait for file to appear
        to_airflow()
    elif export_type == "csv":
        to_json()
        print("Info: Be aware that the Json was not exported to Airflow.")
    else:
        print("Error: Invalid function parameter.")


if __name__ == "__main__":
    param = str(sys.argv[1])
    generate_airflow_variables(export_type=param)



