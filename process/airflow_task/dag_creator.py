#/usr/bin/python
# -*- codning: utf-8 -*-

import json
import logging
from datetime import timedelta, datetime, time
from os.path import join
from typing import List
import os
import sys

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from process.airflow_task.retailer_sync_operator import RetailerSyncOperator, PlanDataSyncOperator
from process.model.retailer_config import OnlineSyncJob, RetailerSyncJob

default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 14, 22, 10),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


def create_retailer_dag(org_code, maintenance_session, json_jobs, retailer_config):
    logging.info("create retailer dag for %s"%org_code)
    total_jobs = maintenance_session.query(OnlineSyncJob)\
        .filter(OnlineSyncJob.org_code == org_code).order_by(OnlineSyncJob.task_priority).all()
    dags = {}
    dags = create_retailer_sync_dags(org_code, total_jobs, dags, retailer_config)
    dags = create_plan_sync_dags(org_code, dags, maintenance_session)
    return dags


def create_retailer_sync_dags(org_code, total_jobs, dags, retailer_config):
    retailer_sync_dag_name = "z_retailer_sync_{0}_ss".format(org_code)
    schedule_interval = "*/{0} 8-23 * * *".format(str(retailer_config.ss_delta))
    if retailer_config.ss_delta == 1:
        schedule_interval = "* 8-23 * * *"
    retailer_sync_dag = DAG(retailer_sync_dag_name, default_args=default_args,
                            schedule_interval=schedule_interval, catchup=False)
    ss_operator = RetailerSyncOperator(
        task_id="retailer_sync_task",
        org_code=org_code,
        execution_timeout=timedelta(minutes=20),
        priority_weight=1,
        # pool=retailer_config.ss_queue_name,
        retries=1,
        retry_delay=timedelta(seconds=15),
        dag=retailer_sync_dag)
    dags[retailer_sync_dag_name] = retailer_sync_dag
    return dags


def create_plan_sync_dags(org_code: str, dags: dict, maintenance_session):
    total_jobs = maintenance_session.query(RetailerSyncJob) \
                        .filter(RetailerSyncJob.org_code == org_code).filter(RetailerSyncJob.mode == "plan").all()

    if len(total_jobs) > 0:
        plan_sync_dag_name = "z_retailer_sync_{0}_plan".format(org_code)
        plan_sync_dag = DAG(plan_sync_dag_name, default_args=default_args,
                            schedule_interval="* 8-23 * * *", catchup=False)

        plan_data_sync_operator = PlanDataSyncOperator(
            task_id = "plan_data_sync",
            org_code = org_code,
            email_on_failure = True,
            email="data@joowing.com",
            dag = plan_sync_dag
        )

        dags[plan_sync_dag_name] = plan_sync_dag

    return dags