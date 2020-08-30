from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from operators import StageToRedshiftOperator, DataQualityOperator
from .functions import (
    create_location_table, create_equipment_notification_table
)

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'udac_example_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    start_date=datetime(2019, 1, 1, 7, 0, 0, 0),
    schedule_interval='@daily'
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_equipment_to_redshift = StageToRedshiftOperator(
    task_id='Stage_equipments',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_path='s3://capstone-project-janga/equipments.json',
    table='equipments'
)

stage_notifications_to_redshift = StageToRedshiftOperator(
    task_id='Stage_notifications',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_path='s3://capstone-project-janga/notifications.json/',
    table='notifications'
)

test_equipments_and_notifications_table = DataQualityOperator(
    task_id='test_equipments_and_notifications_table',
    dag=dag,
    redshift_conn_id='redshift',
    tables=['equipments', 'notifications', ]
)

insert_locations = PythonOperator(
    task_id='Insert_locations',
    dag=dag,
    python_callable=create_location_table
)

insert_equipment_notifications = PythonOperator(
    task_id='Insert_equipment_notifications',
    dag=dag,
    python_callable=create_equipment_notification_table
)

test_locations_and_equipment_notifications_table = DataQualityOperator(
    task_id='test_equipments_and_notifications_table',
    dag=dag,
    redshift_conn_id='redshift',
    tables=['locations', 'equipment_notifications', ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> stage_equipment_to_redshift
start_operator >> stage_notifications_to_redshift

stage_equipment_to_redshift >> test_equipments_and_notifications_table
stage_notifications_to_redshift >> test_equipments_and_notifications_table

test_equipments_and_notifications_table >> insert_locations
test_equipments_and_notifications_table >> insert_equipment_notifications

insert_locations >> test_locations_and_equipment_notifications_table
insert_equipment_notifications >> test_locations_and_equipment_notifications_table

test_locations_and_equipment_notifications_table >> end_operator
