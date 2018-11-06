import importlib
import json

import os
import requests
import time

import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext
from sqlalchemy.orm import Session

from process.service.base_service import BaseService
from process.model.retailer_config import DatabaseConfig, LivyStreamingTask
from process.util.util import name_with_prefix
from process.util.hdfs_script_uploader import *

class LivyRunner(object):
    @staticmethod
    def run(**kwargs):
        session = BaseService.initialize_maintenance_objects()
        config = DatabaseConfig.current_config(session)
        spark_worker_debug = os.environ.get("SPARK_WORKER_DEBUG", "1")
        if spark_worker_debug == "1":
            LivyRunner.local_run(**kwargs)
        else:
            LivyRunner.online_run(**kwargs)

    @staticmethod
    def local_run(name, file, args, py_files=(), skip_log=False, spark_cpu_size=2, record_id=False,
                  executor_memory="6g", driver_memory="8g", memory_overhead="2048"):
        session = BaseService.initialize_maintenance_objects()
        config = DatabaseConfig.current_config(session)
        env_prefix = config.get("prefix", None)
        task_name = name_with_prefix(env_prefix, name)
        hive_ware_path = name_with_prefix(env_prefix, "hive")

        spark = SparkSession \
            .builder \
            .master("local[8]") \
            .appName(task_name) \
            .config("spark.driver.memory", "4G") \
            .config("spark.executor.cores", 8) \
            .config("spark.executor.memory", "8G") \
            .config("spark.sql.warehouse.dir", "/user/{0}/warehouse".format(hive_ware_path)) \
            .config("spark.network.timeout", "360000") \
            .config("spark.executor.heartbeatInterval", "360000") \
            .config("spark.sql.pivotMaxValues", "200000") \
            .enableHiveSupport() \
            .getOrCreate()
        code_dir = os.path.dirname(__file__)
        globals()["spark"] = spark
        first_argv = sys.argv[0]
        all_argv = [first_argv, str(env_prefix), code_dir, json.dumps(config)] + args
        sys.argv.clear()
        for a in all_argv:
            sys.argv.append(a)
        print(file)

        root_path = os.path.dirname(__file__)
        os.makedirs(os.path.join(root_path, "local_run"), exist_ok=True)
        base_file_name = os.path.basename(file)
        only_name = base_file_name.replace(".py", "")
        script_prefix = "def {0}():\r\n".format(only_name)

        target_local_run_file = os.path.join(root_path, "local_run", base_file_name)
        with open(target_local_run_file, "w", encoding="utf8") as f:
            f.write(script_prefix)

            with open(file, "r", encoding="utf8") as code:
                for line in code:
                    if len(file) == 0:
                        f.write("   \r\n")
                    else:
                        f.write("   {0}".format(line))

        f = importlib.import_module("local_run.{0}".format(only_name))
        print(getattr(f, only_name))
        getattr(f, only_name)()

    @staticmethod
    def online_run(name, file, args, py_files=(), skip_log=False, spark_cpu_size=2,
                   record_id=False, executor_memory="6g", driver_memory="8g", memory_overhead="2048"):
        session = BaseService.initialize_maintenance_objects()
        config = DatabaseConfig.current_config(session)
        env_prefix = config.get("prefix", None)
        livy_prefix = config["livy"]["address"]
        task_name = name_with_prefix(env_prefix, name)
        if "fast" in task_name and env_prefix is None:
            livy_prefix = "127.0.0.1:8998"

        # LivyRunner.stop_if_exists(task_name, livy_prefix, session)
        # target_file_name, target_zp_file_name = HdfsScriptUploader.upload(name, file, config)
        target_file_name = HdfsScriptUploader.upload(name, file, config)
        data = {
            "name": task_name,
            "file": target_file_name,
            # "args": [str(env_prefix), target_zp_file_name, json.dumps(config)] + args,
            "args": [str(env_prefix), json.dumps(config)] + args,
            "conf": {
                "spark.cores.max": str(spark_cpu_size),
                "spark.sql.warehouse.dir": "/user/hive/warehouse",
                "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
                "spark.executor.memory": executor_memory,
                "spark.driver.memory": driver_memory,
                "spark.mesos.executor.memoryOverhead": memory_overhead
            }
        }

        r = requests.post("http://{0}/batches".format(livy_prefix), json=data)
        print(r.json())
        result = r.json()
        batch_id = result["id"]
        print("record_id: {0}".format(record_id))
        if record_id:
            LivyStreamingTask.record_livy_streaming_task(session, batch_id, task_name)

        state = "running"
        while state == "running":
            sleep_time = 1
            if skip_log is True:
                sleep_time = 30

            time.sleep(sleep_time)
            r = requests.get("http://{0}/batches/{1}/state".format(livy_prefix, batch_id))
            batch_result = r.json()
            print(batch_result)
            state = batch_result["state"]

        if skip_log is False:
            log_request = requests.get("http://{0}/batches/{1}/log".format(livy_prefix, batch_id),
                                       params={"from": 0, "size": 100000})
            logs = log_request.json()["log"]
            if logs is not None:
                for l in logs:
                    print("livy log: {0}".format(l))

        if state != "success":
            raise "Livy job run into state: {0}, and batch id is: {1}".format(state, batch_id)

    @staticmethod
    def stop_if_exists(name, livy_prefix, session):
        #  type: (str, str, Session) -> None
        batch_id = LivyStreamingTask.load_running_task(session, name)

        if batch_id is not None:
            dr = requests.delete("http://{0}/batches/{1}".format(livy_prefix, str(batch_id)))
            print(dr.json())
            LivyStreamingTask.remove_livy_streaming_task(session, batch_id)


if __name__ == "__main__":
    livyrunner = LivyRunner
    livyrunner.run()