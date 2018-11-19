#/usr/bin/env python
#encoding: utf-8

import hashlib
import json
import os
import tempfile
from datetime import datetime
import time

from os.path import join, basename, abspath

import pymysql

class SyncOperator(object):
    def __init__(self, path, org_code, mode, interface_name):
        """

        :param path:
        :param org_code:
        :param mode:
        :param interface_name:
        """
        self.path = path
        self.org_code = org_code
        self.mode = mode
        self.interface_name = interface_name
        self.config= None
        self.retailer_config = None
        self.connection = None
        self.database_type = None
        self.database_config = None
        self.fetch_size = 0
        self.target_columns = []
        self.target_tables = []
        self.pre_sql = None
        self.session_sql = None
        self.query_sql = None

        self._load_meta()
        self._load_pre_sql()
        self._load_query_sql()
        self._load_session_sql()

    def sync(self):
        self._init_config()
        self._init_connection()
        self.get_database_config()
        self._get_sync_json()
        pass

    def _init_config(self):
        with open(join(self.path, 'config.json'), 'r') as config_content:
            self.config = json.load(config_content)

    def _init_connection(self):
        self.connection = pymysql.connect(host=self.config['host'], port=self.config['port'],
                                          user=self.config['username'], password=self.config['password'],
                                          database=self.config['database_name'], charset='utf8')

    def get_database_config(self):
        cur = self.connection.cursor()
        sql = "select database_type, database_config from retailer_configs where org_code='" + self.org_code + "'"
        cur.execute(sql)
        database_config = cur.fetchone()
        self.database_type = database_config[0]
        self.database_config = json.loads(database_config[1])
        cur.close()
        self.connection.close()

    def _load_meta(self):
        with open(join(self.path, self.org_code, self.mode, self.interface_name, "meta.json"), 'r+') as meta_config:
            meta_config = json.load(meta_config)
            self.target_columns = meta_config.get("target_columns", [])
            self.target_tables = meta_config.get("target_tables", [])
            self.fetch_size = meta_config.get("fetch_size", 1024)

    def _load_pre_sql(self):
        self.pre_sql = self._load_sql("pre_sql.0.sql")

    def _load_query_sql(self):
        self.query_sql = (self._load_sql("query_sql.0.sql"))

    def _load_session_sql(self):
        self.session_sql = (self._load_sql("session_sql.0.sql"))

    def _load_sql(self, name):
        content = open(join(self.path, self.org_code, self.mode, self.interface_name, name), 'r').read()
        return content

    def _get_sync_json(self):
        sql_username = self.database_config['username']
        sql_password = self.database_config['password']
        sql_host = self.database_config['host']
        sql_port = self.database_config['port']
        sql_database = self.database_config['database']
        print(self.database_config)
        if self.database_type == "sql_server":
            name = "sqlserverreader"
            sql_jdbc_url = "jdbc:jtds:sqlserver://{0}:{1};DatabaseName={2}".format(sql_host, sql_port, sql_database)

        if self.database_type == "mysql":
            name = "mysqlreader"
            sql_jdbc_url = "jdbc:mysql://{0}:{1}/{2}".format(sql_host, sql_port, sql_database)
        jw_user = self.config['username']
        jw_pass = self.config['password']
        jw_host = self.config['host']
        jw_port = self.config['port']
        online_dw_name = self.org_code + "dw"
        jw_jdbc_url = "jdbc:mysql://{0}:{1}/{2}?useUnicode=true&characterEncoding=utf-8" \
                            .format(jw_host, jw_port, online_dw_name)
        datax_json = {
            "timestamp": int(round(time.time() * 1000)),
            "job": {
                "setting": {"speed": {"byte": 1048576}},
                "content": [
                    {
                        "reader": {
                            "name": name,
                            "parameter": {
                                "username": sql_username,
                                "password": sql_password,
                                "connection": [
                                    {
                                        "querySql": [self.query_sql],
                                        "jdbcUrl": [sql_jdbc_url]
                                    }
                                ],
                                "fetchSize": 1024
                            }
                        },
                        "writer": {
                            "name": "mysqlwriter",
                            "parameter": {
                                "writeMode": "insert",
                                "username": jw_user,
                                "password": jw_pass,
                                "column": self.target_columns,
                                "session": [self.session_sql],
                                "preSql": [self.pre_sql],
                                "connection": [
                                    {
                                        "jdbcUrl": jw_jdbc_url,
                                        "table": self.target_tables
                                    }
                                ]
                            }
                        }
                    }
                ]
            }
        }
        datax_path = os.environ.get("DATAX_HOME")
        temp = tempfile.NamedTemporaryFile(mode="w+")
        temp.write(json.dumps(datax_json, ensure_ascii=False))
        temp.flush()
        bash_command = "{0}/bin/datax.py {1}".format(datax_path, temp.name)
        print(os.system(bash_command))



org_code = 'yyplan'
mode = 'plan'
online_path = abspath("./job")

# SyncOperator(online_path, org_code, mode, 'tmp_shop').sync()
# SyncOperator(online_path, org_code, mode, 'tmp_member').sync()
# SyncOperator(online_path, org_code, mode, 'tmp_product').sync()
# SyncOperator(online_path, org_code, mode, 'tmp_sales_item').sync()
# SyncOperator(online_path, org_code, mode, 'tmp_billing').sync()
# SyncOperator(online_path, org_code, mode, 'tmp_billing_coupon').sync()
# SyncOperator(online_path, org_code, mode, 'tmp_guider').sync()
# SyncOperator(online_path, org_code, mode, 'tmp_payment').sync()

SyncOperator(online_path, org_code, mode, 'tmp_plan').sync()