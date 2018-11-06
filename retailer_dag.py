# coding=utf-8

import os
import sys
import re

sys.path.append(os.path.dirname(__file__))

from airflow import DAG
from process.airflow_task.dag_creator import create_retailer_dag
from process.model.retailer_config import RetailerConfig, DatabaseConfig
from process.service.base_service import BaseService

maintenance_session = BaseService.initialize_maintenance_objects()
retailer_configs = maintenance_session.query(RetailerConfig) \
    .filter(RetailerConfig.activated.is_(True)).all()

action = None
dag_name = None
detected_org_code = None
ss_re = re.compile("^z_retailer_sync_(.*)_(ss|month|dr|stock|_spark_import|_data_deal)$")
skip_all_retailer_dags = False

if len(sys.argv) >= 3:
    action = sys.argv[1]
    dag_name = sys.argv[2]

    if action == "run" or action == "test":
        skip_all_retailer_dags = not dag_name.startswith("z_retailer_sync_")
        matched_org_codes = ss_re.findall(dag_name)
        if len(matched_org_codes) == 1:
            detected_org_code = matched_org_codes[0][0]

if not skip_all_retailer_dags:
    for retailer_config in retailer_configs:
        if detected_org_code is not None and retailer_config.org_code != detected_org_code:
            continue

        jobs = None

        retailer_dags = create_retailer_dag(retailer_config.org_code, maintenance_session, jobs, retailer_config)

        for key in retailer_dags:
            globals()[key] = retailer_dags[key]

