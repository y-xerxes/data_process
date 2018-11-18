# -*- coding: utf-8 -*-

import datetime
import json
import logging
import sys
from typing import Iterable, Dict, Union, List

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from jinja2 import Template
from sqlalchemy import and_
from sqlalchemy.orm import Session

from process.airflow_task.service.mysql_executor import RetailerMySqlExecutor
from process.model.retailer_config import DatabaseConfig, RetailerSyncJob, RetailerConfig
from process.datax_service.datax_service_helper import DataxServiceHelper
from process.service.base_service import BaseService
from process.service.kafka_executor import KafkaExecutor


class RetailerSyncOperator(BaseOperator):
    @apply_defaults
    def __init__(self, org_code: str, **kwargs):
        super(RetailerSyncOperator, self).__init__(**kwargs)
        self.org_code = org_code
        self._maintenance_session = None
        self._database_config = None
        self._retailer_config = None
        self.retailer_sync_jobs = []

    def _load_retailer_sync_jobs(self):
        """
        获取商户的ss任务
        """
        self.retailer_sync_jobs = self.maintenance_session.query(RetailerSyncJob) \
            .filter(and_(RetailerSyncJob.org_code == self.org_code, RetailerSyncJob.mode == "ss")) \
            .order_by(RetailerSyncJob.task_priority).all()

    def execute(self, context):
        self._load_retailer_sync_jobs()
        # if len(self.retailer_sync_jobs) > 0:
        #     self._execute_job_sync()
        #     self._select_lock_status_process()
        #     self._execute_prt_sales_process()
        #     self._execute_ss_tmp()
        self._send_kafka_notify()
        # self._send_metrics()
        # self._call_sp_deal()

    def _execute_job_sync(self) -> None:
        """
        顺序执行ss的同步任务
        """

        for retailer_sync_job in self.retailer_sync_jobs:
            RetailerSyncJobExecutor(
                maintenance_session=self.maintenance_session,
                database_config=self.database_config,
                retailer_config=self.retailer_config,
                retailer_sync_job=retailer_sync_job
            ).exec_job()

    def _send_kafka_notify(self):
        topic_name = "fast_ss_notify1"
        time_format = "%Y-%m-%d %X"
        msg_body = json.dumps({"org_code": self.org_code,
                               "dw_name": self.retailer_config.online_dw_name,
                               "time": datetime.datetime.now().strftime(time_format)})
        KafkaExecutor(config=self.database_config).send_message(topic_name=topic_name, msg=msg_body)

    @property
    def maintenance_session(self) -> Session:
        if self._maintenance_session is None:
            self._maintenance_session = BaseService.initialize_maintenance_objects()

        return self._maintenance_session

    @property
    def database_config(self):
        if self._database_config is None:
            self._database_config = DatabaseConfig.current_config(self.maintenance_session)

        return self._database_config

    @property
    def retailer_config(self):
        if self._retailer_config is None:
            self._retailer_config = self.maintenance_session.query(RetailerConfig)\
                .filter(RetailerConfig.org_code == self.org_code).first()
        return self._retailer_config



class RetailerSyncJobExecutor(object):
    """
    从RetailerSyncJob中剥离出增量同步相关的机制, 做更多的功能:
    1. 增量同步之后, 将增量同步的数据广播出来
    """
    def __init__(self,
                 maintenance_session: Session,
                 database_config: Dict,
                 retailer_config: RetailerConfig,
                 retailer_sync_job: RetailerSyncJob):
        super(RetailerSyncJobExecutor, self).__init__()
        self.maintenance_session = maintenance_session
        self.database_config = database_config
        self.retailer_config = retailer_config
        self.retailer_sync_job = retailer_sync_job
        self.remote_field = None
        self.local_field = None
        self.notify_change = False
        self.datax_service_host = self.database_config.get("datax_service_host", None)

    def exec_job(self, ):
        self.try_to_read_config()
        if self.is_support_increment_sync():
            self._increment_sync()
        else:
            self._direct_sync(self.retailer_sync_job.query_sql)

    def try_to_read_config(self):
        """
        # 读取商户增量同步的配置
        """
        if self.retailer_sync_job.increment_config is not None and \
            len(self.retailer_sync_job.increment_config.keys()) > 0:
            self.remote_field = self.retailer_sync_job.increment_config.get("remote_field", None)
            self.local_field = self.retailer_sync_job.increment_config.get("local_field", None)
            self.notify_change = self.retailer_sync_job.increment_config.get("notify_change", False)

    def is_support_increment_sync(self):
        """
        # 判断是否可以做增量同步
        """
        return self.remote_field is not None and self.local_field is not None

    def _increment_sync(self):
        current_max_value = self.read_max_value()
        max_condition_pit = self._build_increment_sync_remote_query_pit(max_value=current_max_value)
        result_sqls = []
        for one_query_sql in self.retailer_sync_job.query_sql:
            result_sqls.append(Template(one_query_sql).render(max_condition_pit=max_condition_pit))

        self._direct_sync(result_sqls)
        if self.notify_change:
            self._notify_change(current_max_value)

    def read_max_value(self):
        current_max_value = None
        table_name = self.retailer_sync_job.target_tables[0]
        max_value_load_sql = "select max{0} as max_value from {1}".format(self.local_field, table_name)
        max_results = RetailerMySqlExecutor(
            maintenance_session = self.maintenance_session,
            database_config = self.database_config,
            org_code = self.retailer_sync_job.org_code,
            sql = max_value_load_sql
        ).read()

    def _build_increment_sync_remote_query_pit(self, max_value, str):
        return self._build_increment_sync_query_pit(max_value=max_value, field_name=self.remote_field)

    def _build_increment_sync_query_pit(max_value: str, field_name):
        if max_value is None:
            return "1 = 1"
        else:
            return "{0} > '{1}'".format(field_name, str(max_value))

    def _direct_sync(self, query_sql: List[str]):
        datax_job = self.retailer_sync_job.datax_config(
            retailer_config = self.retailer_config,
            config = self.database_config,
            query_sql = query_sql
        )
        DataxServiceHelper.exec_datax_job(job_config=datax_job, datax_service_host=self.datax_service_host)

    def _notify_change(self, max_value: str):
        table_name = self.retailer_sync_job.target_tables[0]
        increment_local_query_sql = self._build_increment_sync_local_query_pit(max_value)
        increment_synced_value_load_sql = "select * from {0} where {1}".format(table_name, increment_local_query_sql)
        increment_synced_value = list(RetailerMySqlExecutor(
            maintenance_session = self.maintenance_session,
            database_config = self.database_config,
            org_code = self.retailer_sync_job.org_code,
            sql = increment_synced_value_load_sql
        ).read())[0]

    def _build_increment_sync_local_query_pit(self, max_value, str):
        return self._build_increment_sync_local_query_pit(max_value=max_value, field_name=self.local_field)


class PlanDataSyncOperator(BaseOperator):
    @apply_defaults
    def __init__(self, org_code: str, **kwargs):
        super(PlanDataSyncOperator, self).__init__(**kwargs)
        self.org_code = org_code
        self._maintenance_session = None
        self._database_config = None
        self._retailer_config = None
        self.plan_sync_jobs = []

    def execute(self, context):
        self._load_plan_sync_jobs()
        self._execute_job_sync()

    @property
    def database_config(self) -> dict:
        if self._database_config is None:
            self._database_config = DatabaseConfig.current_config(self.maintenance_session)
        return self._database_config

    @property
    def maintenance_session(self) -> Session:
        if self._maintenance_session is None:
            self._maintenance_session = BaseService.initialize_maintenance_objects()
        return self._maintenance_session

    @property
    def retailer_config(self) -> RetailerConfig:
        if self._retailer_config is None:
            self._retailer_config = self.maintenance_session.query(RetailerConfig)\
                .filter(RetailerConfig.org_code == self.org_code).first()
            return self._retailer_config

    def _load_plan_sync_jobs(self):
        self.plan_sync_jobs = self.maintenance_session.query(RetailerSyncJob) \
                        .filter(RetailerSyncJob.org_code == self.org_code).filter(RetailerSyncJob.mode == "plan").all()

    def _execute_job_sync(self):
        for plan_sync_job in self.plan_sync_jobs:
            PlanSyncJobExecutor(
                maintenance_session=self.maintenance_session,
                database_config=self.database_config,
                retailer_config=self.retailer_config,
                plan_sync_job=plan_sync_job
            ).exec_job()


class PlanSyncJobExecutor(object):
    def __init__(self,
                 maintenance_session: Session,
                 database_config: Dict,
                 retailer_config: RetailerConfig,
                 plan_sync_job: RetailerSyncJob):
        super(PlanSyncJobExecutor, self).__init__()
        self.maintenance_session = maintenance_session
        self.database_config = database_config
        self.retailer_config = retailer_config
        self.plan_sync_job = plan_sync_job
        self.remote_field = None
        self.local_field = None
        self.notify_change = False
        self.datax_service_host = self.database_config.get("datax_service_host", None)

    def exec_job(self):
        self.read_config()
        if self.is_support_increment_sync():
            self._increment_sync()
        else:
            self._direct_sync(self.plan_sync_job.query_sql)

    def read_config(self):
        """
        读取增量同步相关的字段数据
        :return:
        """
        pass

    def is_support_increment_sync(self):
        """
        判断是否可以做增量同步
        :return:
        """
        pass

    def _increment_sync(self):
        pass

    def _direct_sync(self, query_sql: List[str]):
        pass