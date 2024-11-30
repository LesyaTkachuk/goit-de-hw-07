from airflow import DAG
from datetime import datetime
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule as tr
from airflow.utils.state import State
from enum import Enum
import random
import time

# default DAG arguments
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 11, 30, 0, 0),
}

# name of the database connection (MySQL)
connection_name = "SQL_GOIT"


# define medals Enum
class Medals(Enum):
    BRONZE = "Bronze"
    SILVER = "Silver"
    GOLD = "Gold"


# list of medal types
medals_list = [el.value for el in Medals]


# define function to choose random medal type
def choose_medal(ti):
    medal_type = random.choice(medals_list)
    print(f"Medal type: {medal_type}")

    return medal_type


# define function to choose the next task depending on the medal type
def pick_medal_type(ti):
    medal_type = ti.xcom_pull(task_ids="pick_medal")
    print(f"Medal type recieved: {medal_type}")

    if medal_type == Medals.BRONZE.value:
        return "calc_Bronze"
    elif medal_type == Medals.SILVER.value:
        return "calc_Silver"
    elif medal_type == Medals.GOLD.value:
        return "calc_Gold"
    else:
        return None


# create DAG context
with DAG(
    "hw_07_mysql_oleksandra",
    default_args=default_args,
    schedule_interval=None,  # DAG doesn't have a schedule
    # schedule_interval="*/2 * * * *",  # DAG runs every 2 minutes
    catchup=False,  # Вимкнути запуск пропущених задач
    tags=["oleksandra_tk"],  # Теги для класифікації DAG
) as dag:
    # task to create database (if not exists)
    create_schema = MySqlOperator(
        task_id="create_schema",
        mysql_conn_id=connection_name,
        sql="""
        CREATE DATABASE IF NOT EXISTS oleksandra_tk;
        """,
    )

    # task to create table (if not exists)
    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id=connection_name,
        sql="""
        CREATE TABLE IF NOT EXISTS oleksandra_tk.games (
        `id` int AUTO_INCREMENT PRIMARY KEY,
        `medal_type` text,
        `count` int DEFAULT NULL,
        `created_at` text
        );
        """,
    )

    # task to pick medal type
    pick_medal = PythonOperator(
        task_id="pick_medal",
        python_callable=choose_medal,
    )

    # task to calculate the number of the Bronze medals and write it to the new table
    calc_Bronze = MySqlOperator(
        task_id="calc_Bronze",
        mysql_conn_id=connection_name,
        sql=""" INSERT INTO oleksandra_tk.games (medal_type, count, created_at) 
                VALUES ('Bronze',
                        (SELECT Count(*) FROM olympic_dataset.athlete_event_results  WHERE medal="Bronze"),  
                        NOW() )""",
    )

    # task to calculate the number of the Silver medals and write it to the new table
    calc_Silver = MySqlOperator(
        task_id="calc_Silver",
        mysql_conn_id=connection_name,
        sql=""" INSERT INTO oleksandra_tk.games (medal_type, count, created_at) 
                VALUES ('Silver',
                        (SELECT Count(*) FROM olympic_dataset.athlete_event_results WHERE medal="Silver"),  
                        NOW() )""",
    )

    # task to calculate the number of the Gold medals and write it to the new table
    calc_Gold = MySqlOperator(
        task_id="calc_Gold",
        mysql_conn_id=connection_name,
        sql=""" INSERT INTO oleksandra_tk.games (medal_type, count, created_at) 
                VALUES ('Gold',
                        (SELECT Count(*) FROM olympic_dataset.athlete_event_results WHERE medal="Gold"),  
                        NOW() )""",
    )

    # task to choose the next task for execution based on the previous task result
    pick_medal_task = BranchPythonOperator(
        task_id="pick_medal_task", python_callable=pick_medal_type
    )

    # task to generate delay if all previous task were successful
    generate_delay = PythonOperator(
        task_id="generate_delay",
        python_callable=lambda: time.sleep(15),
        #  to test failed final task set the delay value greater than 30
        # python_callable=lambda: time.sleep(35),
        trigger_rule=tr.ONE_SUCCESS,
    )

    # task to check
    check_correctness = SqlSensor(
        task_id="check_correctness",
        conn_id=connection_name,
        sql="SELECT (SELECT MAX(created_at)>(NOW()-INTERVAL 30 SECOND) FROM oleksandra_tk.games) FROM oleksandra_tk.games ",
        poke_interval=10,
        timeout=30,
        mode="reschedule",
    )

    # define dependencies between tasks
    create_schema >> create_table >> pick_medal >> pick_medal_task

    pick_medal_task >> [calc_Bronze, calc_Silver, calc_Gold]

    calc_Bronze >> generate_delay >> check_correctness
    calc_Silver >> generate_delay >> check_correctness
    calc_Gold >> generate_delay >> check_correctness
