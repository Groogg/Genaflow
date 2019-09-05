
# TL;TR
*** This repository is censored from private data and is simply a snapshot of the real project (taken on the 2019-09-05). The idea is to present what have been done.
Genaflow is an [Apache Airflow](https://airflow.apache.org/) data pipeline. At the moment, here is the Airflow instance description:  
-   Runs locally on your machine
-   Uses SQLite to store Airflow metadata
-   Uses Airflow’s `Sequential Executor` to run tasks

# Setup  
  
## Before installation (python required)
  
Confirm you have Python 3 installed with  `python3 --version`.  
  
To make sure that our versions are up-to-date, let’s update and upgrade the system with apt-get:
```
$ sudo apt-get update
$ sudo apt-get -y upgrade
```
To manage software packages for Python, let’s install pip:
```
$ sudo apt-get install -y python3-pip
```
There are a few more packages and development tools to install to ensure that we have a robust set-up for our programming environment: 
```
sudo apt-get install build-essential libssl-dev libffi-dev python-dev
```
## Installation

First things first: let’s set up our local project.

Open up your terminal and navigate to the directory you want your project to live in. Make a new directory for your project and enter it.
```
$ cd ~    
$ mkdir genaflow 
$ cd genaflow
```
or 
```
$ cd ~    
$ git clone https://github.com/GreggGilbert/genaflow.git 
$ cd genaflow
```

## Install and setup virtualenv
We now need a Python virtual environment to manage our project and its dependencies.
```
$ pip3 install virtualenv  
$ python3 -m venv pyvenv
$ source pyvenv/bin/activate
```
Now let’s install Airflow with the *crypto* packages that allows the password to be hashed and *mssql* package that offers Microsoft SQL Server operators and hooks: 
```
(pyvenv) $ export SLUGIFY_USES_TEXT_UNIDECODE=yes  
(pyvenv) $ pip install apache-airflow[crypto,mssql]==1.10.2
```

## Airflow configurations

First, we need to set the AIRFLOW_HOME environment variable. This will specify the path (it doesn’t need to exist yet) where Airflow stores things like our airflow.cfg file, the SQLite metadata database, logfiles, etc. The directory is already created when pulling the repository. Run
```
(pyvenv) $ export AIRFLOW_HOME=`pwd`/airflow_home
```
For ease of use, it's preferable to remove the DAG examples (since we know what we are doing). To do so, open the airflow.cfg file and change the variable `load_examples` to False.

Now that Airflow knows _where_ to put our metadata database, let’s create it. Airflow’s CLI provides a straightforward way to setup a simple SQLite database that comes with everything we need right out of the box.
```
(pyvenv) $ airflow initdb
```
Run `airflow version` to see if it was properly installed. If the command worked, then Airflow also created its default configuration file `airflow.cfg` in `AIRFLOW_HOME`:
```
airflow_home
├── airflow.cfg
└── airflow.db
└── unittests.cfg
└── dags/
```
### Generate the Fernet Key

The fernet key is used to read the encrypted data hashed by the `crypto` modules. In order to generate the key run 
```
(pyvenv) $ python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```
This will print a key, *copy* it, open the `airflow.cfg` file and replace the `fernet_key` (close to line 106) value with the new one.

### Import the Airflow variables

The application needs to access config variables that are stored in the Airflow metadata database. Replace the `PATH` variables at the top of the file with your path. This file will be able to create a json file with the proper variables for the application to work and will export them to the Airflow metadata database. Run from the project directory:
```
(pyvenv) $ python3 scripts/generate_airflow_variables.py airflow
```

### Import the Airflow connections

The application needs to access an SQL database and the database URL and credentials are stored in the Airflow metadata database. In order to add the connection to the SQL Server database, run 
```
(pyvenv) $ airflow connections -a --conn_id asterix_test --conn_uri mssql+pyodbc://USERNAME:PASSWORD@192.168.12.10/DATABASE_NAME
```
by replacing `USERNAME` and `PASSWORD` with your actual credentials.

### Configure the email system
When Airflow faces error it will send emails. In order to do so, create a google App Password for your gmail account. This is done so that you don't use your original password or 2 Factor authentication.

1.  Visit your [App passwords](https://security.google.com/settings/security/apppasswords) page. You may be asked to sign in to your Google Account.
2.  At the bottom, click **Select app** and choose the app you’re using.
3.  Click **Select device** and choose the device you’re using.
4.  Select **Generate**.
5.  Follow the instructions to enter the App password (the 16 character code in the yellow bar) on your device.
6.  Select **Done**.

Once you are finished, you won’t see that App password code again. However, you will see a list of apps and devices you’ve created App passwords for.

Edit `airflow.cfg` and edit the `[smtp]` section as shown below:

```
[smtp]
smtp_host = smtp.gmail.com
smtp_starttls = True
smtp_ssl = False
smtp_user = YOUR_EMAIL_ADDRESS
smtp_password = 16_DIGIT_APP_PASSWORD
smtp_port = 587
smtp_mail_from = YOUR_EMAIL_ADDRESS
```

Edit the below parameters to the corresponding values:

`YOUR_EMAIL_ADDRESS` = Your Gmail address  
`16_DIGIT_APP_PASSWORD` = The App password generated above

## Other configurations

### Install Microsoft ODBC Driver for SQL Server 
You will need the `curl` package to download the ODBC Driver, to do so run :
```
apt install -y curl
```

Follow the steps listed in the [official Microsoft documentation](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-2017) and make sure to install the optional: *for unixODBC development headers package*.

### Python package
Install the packages defined in *requirements.txt*:
```
(pyvenv) $ export SLUGIFY_USES_TEXT_UNIDECODE=yes   
(pyvenv) $ pip install -r requirements.txt
```

### Install Chrome and the webdriver

Install Chrome
```
wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | sudo apt-key add -
echo 'deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main' | sudo tee /etc/apt/sources.list.d/google-chrome.list
sudo apt-get update 
sudo apt-get install google-chrome-stable
```
Install the driver from the [latest stable release](http://chromedriver.chromium.org/home) and install it.
```
wget https://chromedriver.storage.googleapis.com/2.41/chromedriver_linux64.zip
unzip chromedriver_linux64.zip

sudo mv chromedriver /usr/bin/chromedriver
sudo chown root:root /usr/bin/chromedriver
sudo chmod +x /usr/bin/chromedriver
```

### Firefox (if needed)
If needed the web driver for firefox can be found [here](https://github.com/mozilla/geckodriver/releases).


# Usage

Run this command in a terminal
```
(pyvenv) $ airflow webserver
```
and this command in another terminal 
```
(pyvenv) $ airflow scheduler
```
The first command starts the UI in the web browser(`http://localhost:8080/admin/`) and the second one start the `scheduler` who's responsible to start and manage the tasks.
After that, sit back and enjoy ;)!


# Known errors and solutions

## xlrd.compdoc.CompDocError 

If this error appear mostly during the uni_etl, please follow theses steps :

1. Open the `compdoc.py` file, in my case `home/genacol/projects/genaflow/pyvenv/lib/python3.6/site-packages/xlrd/compdoc.py`.
2. Comment out this section (around line 425):
```
if self.seen[s]:
    print("_locate_stream(%s): seen" % qname, file=self.logfile); dump_list(self.seen, 20, self.logfile)
    raise CompDocError("%s corruption: seen[%d] == %d" % (qname, s, self.seen[s]))
```



