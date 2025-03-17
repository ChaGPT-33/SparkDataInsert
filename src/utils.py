from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import IntegrityError
from datetime import datetime
from alive_progress import alive_bar
import logging 
import time

dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
dt_string = datetime.now().strftime("%Y%m%d%H%M%S")
exe_log_path = f'./logs/{dt_string}.log'

class Logger:
    instance = None

    def __new__(cls, logger_name = __name__):
        if cls.instance is None:
            cls.instance = super().__new__(cls)
            cls.instance.logger = logging.getLogger(logger_name)
            cls.instance.logger.setLevel(logging.INFO)
            file_handler = logging.FileHandler(exe_log_path, encoding = 'utf-8')
            file_handler.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s - %(name)s - [%(levelname)s]: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
            file_handler.setFormatter(formatter)
            cls.instance.logger.addHandler(file_handler)
        return cls.instance

    def show_logs(self):
        with open(exe_log_path, "r", encoding = 'utf-8') as f:
            content = f.read()
            return content

class MySQLUtils:
    def __init__(self, driver, db_user, db_password, host, port, given_db):

        self.mysql_loggin_info = {
            'drivername': driver,
            'username': db_user,
            'password': db_password,
            'host': host,
            'port': port,
            'database': given_db
            }
        
        self.engine_mysql = None

    def connect_to_mysql(self):
        loggin_url = URL.create(**self.mysql_loggin_info)
        self.engine_mysql = create_engine(loggin_url)

    def insert_dataframe_to_sql(self, df, table_name, primary_keys:list):
        #timer 
        start_time = time.time()

        # main functiomn
        columns = "`,`".join(df.columns)
        
        insert = 0
        update = 0
        error = 0

        with alive_bar(len(df)) as bar:
            for i in range(len(df)):
                bar()   
                data_list = [str(item).replace("'", "") for item in df.iloc[i].to_list()]
                data = "','".join(data_list)
                insert_sql = f"insert into `{table_name}` (`{columns}`) values ('{data}')"
                insert_sql = insert_sql.replace("'null'", "null")

                try:
                    self.engine_mysql.execute(insert_sql)
                    insert += 1

                except IntegrityError as e: # if exists, update sql
                    print("existed, updating")
                    query_list = []
                    for m in range(len(df.iloc[i])):
                        column = df.columns[m]
                        if df.iloc[i][column] != 'NaT' and column not in primary_keys:
                            query_list.append(f"`{column}` = '{df.iloc[i][column]}'")
                    
                    primary_keys_condition = ""
                    for pk in primary_keys:
                        if primary_keys_condition:
                            primary_keys_condition += f" and {pk} = '{df.iloc[i][pk]}'"
                        else:
                            primary_keys_condition += f"{pk} = '{df.iloc[i][pk]}'"

                    update_sql = f"update `{table_name}` set {','.join(query_list)} where {primary_keys_condition}"
                    update_sql = update_sql.replace("'null'", "null")
                    try:
                        self.engine_mysql.execute(update_sql)
                        update += 1
                    except Exception as e:
                        print(f'Data not updated! Error term: {e}')
                        error += 1
        
                except Exception as e:
                    print(f'Data not uploaded! Error term: {e}')
                    error += 1
        # End timer
        end_time = time.time()
        duration = round(end_time - start_time, 2)
        print(f"Execution time: {duration} seconds")

        return insert, update, error, duration