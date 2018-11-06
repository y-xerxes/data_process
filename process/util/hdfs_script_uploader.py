"""
负责将本地脚本上传到hdfs，返回hdfs的目录
"""

import time
import os
from hdfs import Client

class HdfsScriptUploader(object):
    @staticmethod
    def upload(name, file_path, config):
        env_prefix = config.get("prefix", None)
        hdfs_client = Client(url=config["hdfs"]["name_node"])
        hdfs_hosts = []
        hdfs_http_host = config["hdfs"]["name_node"]
        hdfs_hosts.append(hdfs_http_host.replace("http://", ""))
        hdfs_data_service_root = "/data_service"
        if env_prefix is not None:
            hdfs_data_service_root = "/{0}_data_service".format(env_prefix)

        hdfs_client.makedirs(hdfs_data_service_root)
        timestamp = int(round(time.time() * 1000))
        target_file_name = "{2}/{0}/{1}/{0}_{1}.py".format(name, str(timestamp), hdfs_data_service_root)
        hdfs_client.makedirs("{2}/{0}/{1}".format(name, str(timestamp), hdfs_data_service_root))
        print("hdfs file name: {0}".format(target_file_name))
        hdfs_client.upload(target_file_name, file_path)
        zip_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                                "joowing.zip")
        target_zp_file_name = "{2}/{0}/{1}/joowing.zip".format(name, str(timestamp), hdfs_data_service_root)
        # hdfs_client.upload(target_zp_file_name, zip_path)
        # return target_file_name, target_zp_file_name
        return target_file_name
