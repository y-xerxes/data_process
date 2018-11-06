import json
from os.path import join
from typing import List
import os
import sys
from pyspark.streaming import StreamingContext, DStream
from pyspark.streaming.kafka import KafkaUtils

from process.livy_runner import LivyRunner
from process.model.retailer_config import DatabaseConfig
from process.service.base_service import BaseService

from process.util.util import *

class StreamingHelper(object):
    @staticmethod
    def run_streaming_job(name, file_path, argv=(), cpu_size=2, driver_memory='2g', executor_memory='2g'):
        joowing_path = os.path.dirname(__file__)
        full_file_name = join(joowing_path, file_path)
        LivyRunner.run(
            name=name, file=full_file_name, skip_log=True, args=argv, spark_cpu_size=cpu_size, record_id=True,
            executor_memory=executor_memory, driver_memory=driver_memory, memory_overhead="2048"
        )

    @staticmethod
    def load_streaming_context(ssc, name, topics):
        # type: (StreamingContext, str, List[str]) -> DStream
        maintenance_session = BaseService.initialize_maintenance_objects()
        database_config = DatabaseConfig.current_config(maintenance_session)
        bootstrap_servers = database_config["kafka"]["bootstrap_servers"]
        return KafkaUtils.createDirectStream(ssc=ssc, kafkaParams={
            "bootstrap.servers": bootstrap_servers
        }, topics=topics)