import json
import logging
from pprint import pformat

import requests
from airflow import AirflowException

class DataxServiceHelper(object):
    @staticmethod
    def exec_datax_job(job_config: dict, datax_service_host=None):
        if datax_service_host is None:
            datax_service_host = "http://127.0.0.1:9999"

        logging.info("开始执行DataxJob: {0}".format(job_config))
        r = requests.post(
            "{0}/api/job_executions.json".format(datax_service_host),
            data=json.dumps(job_config, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json;charset=UTF-8"}
        )
        logging.info("通过http调用JobExecution返回:")
        logging.info(pformat(r.json()))
        if r.status_code != 200:
            raise AirflowException("调用DataX同步任务失败!")

