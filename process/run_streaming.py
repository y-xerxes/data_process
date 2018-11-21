import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from process.model.retailer_config import DatabaseConfig
from process.service.base_service import BaseService
from process.streaming_helper import StreamingHelper

session = BaseService.initialize_maintenance_objects()
config = DatabaseConfig.current_config(session)
env_prefix = config.get("prefix", None)
cpu_size = 2
driver_memory = "2g"
executor_memory = "2g"
if env_prefix is None:
    hour = datetime.datetime.now().hour
    if 7 <= hour <= 23:
        cpu_size = 16
        driver_memory = "48g"
        executor_memory = "6g"
    else:
        cpu_size = 6
        driver_memory = "2g"
        executor_memory = "2g"

StreamingHelper.run_streaming_job("fast_ss", "streaming.py", [],
                                  cpu_size=cpu_size, driver_memory=driver_memory, executor_memory=executor_memory)