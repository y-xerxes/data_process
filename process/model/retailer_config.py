import datetime
import time
import json
from os.path import join
from typing import List, Union
import MySQLdb
import logging
import sys

from sqlalchemy import Column, String, Integer, JSON, create_engine, Index, DateTime, Text, Boolean, inspect, \
    ForeignKey, or_
from sqlalchemy.orm import sessionmaker, Session, relationship, joinedload
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import TEXT, VARCHAR

from process.service.base_service import BaseService
from process.airflow_task.service.mysql_executor import RetailerMySqlExecutor

MaintenanceBase = declarative_base()

class DatabaseConfig(MaintenanceBase):
    __tablename__ = "database_configs"

    id = Column(Integer, autoincrement=True, nullable=False, primary_key=True)
    config = Column(JSON)

    @staticmethod
    def current_config(session: Session):
        dc = session.query(DatabaseConfig).first()
        return dc.config

    @staticmethod
    def current_database_config(session: Session):
        dc = session.query(DatabaseConfig).first()
        return dc


class RetailerConfig(MaintenanceBase):
    """
    在数据库中存储了商户的sql server的连接配置, 读取并构建商户的数据库配置
    可以控制是否是激活的
    """

    __tablename__ = 'retailer_configs'

    id = Column(Integer, autoincrement=True, nullable=False, primary_key=True)
    org_code = Column(String(32), nullable=False)
    database_type = Column(String(64), nullable=False, default='sql_server')
    database_config = Column(JSON)
    activated = Column(Boolean, nullable=False, default=True)
    database_migrated = Column(Boolean, nullable=False, default=False, server_default="0")
    config_self_managed = Column(Boolean, nullable=False, default=False, server_default='0')
    online_dw_name = Column(String(64), nullable=False, doc="商户DW名称")
    ss_enabled = Column(Boolean(), nullable=False, server_default="0")
    guider_screen = Column(Boolean(), nullable=False, server_default="0")
    super_data_new_api_enabled = Column(Boolean(), nullable=False, server_default="0")
    fast_stream = Column(Boolean(), nullable=False, server_default="0")
    ss_delta = Column(Integer(), nullable=False, server_default="20")
    ss_queue_name = Column(VARCHAR(64), nullable=False, server_default="retailer_sync")
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    erp_brand_id = Column(Integer, nullable=False)

    retailer_sync_jobs = relationship("RetailerSyncJob", back_populates="retailer")
    retailer_sqls = relationship("RetailerSQL", back_populates="retailer")

    @staticmethod
    def dw_name(org_code, session):
        # type: (str, Session) -> str
        rc = session.query(RetailerConfig).filter(RetailerConfig.org_code == org_code).first()  # type: RetailerConfig
        return rc.online_dw_name

    @staticmethod
    def online_dw_names(session):
        # type: (Session) -> list
        online_dw_names = session.query(RetailerConfig.online_dw_name).filter(RetailerConfig.activated == 1).all()
        return online_dw_names

    @staticmethod
    def create_or_update_by(org_code, db_config, session):
        # type: (str, dict, Session) -> None
        rc = session.query(RetailerConfig).filter(RetailerConfig.org_code == org_code).first()  # type: RetailerConfig
        if rc is None:
            session.begin()

            retailer_config = RetailerConfig(org_code=org_code,
                                             database_type=db_config.get('database_type', 'sql_server'),
                                             database_config=db_config,
                                             activated=False,
                                             online_dw_name="{0}DW".format(org_code),
                                             created_at=datetime.datetime.now(),
                                             updated_at=datetime.datetime.now())
            session.add(retailer_config)
            session.commit()
        else:
            old_config = json.dumps(rc.database_config, sort_keys=True)
            new_config = json.dumps(db_config, sort_keys=True)
            if old_config != new_config and not rc.config_self_managed:
                session.begin()
                rc.database_config = db_config
                rc.database_type = db_config['database_type']
                rc.updated_at = datetime.datetime.now()
                session.add(rc)
                session.commit()
            else:
                print("商户[{0}]的数据库配置未更新".format(org_code))

    @staticmethod
    def sync_online_retailers(session):
        # type: (Session) -> None
        all_retailer_configs = session.query(RetailerConfig.org_code)

        logging.info(u"删除所有的已经被删除掉的商户的job")
        session.query(OnlineSyncJob)\
            .filter(OnlineSyncJob.org_code.notin_(all_retailer_configs))\
            .delete(synchronize_session=False)

        logging.info(u"删除所有已经被删除掉商户的SyncJob")
        session.query(RetailerSyncJob) \
            .filter(RetailerSyncJob.org_code.notin_(all_retailer_configs)) \
            .delete(synchronize_session=False)

        logging.info(u"删除所有已经被删除掉OnlineSyncJob的任务")
        session.query(OnlineSyncJob)\
            .filter(OnlineSyncJob.retailer_sync_job_id.notin_(session.query(RetailerSyncJob.id))) \
            .delete(synchronize_session=False)

        logging.info(u"新增任务同步进去")
        sub_query = session.query(OnlineSyncJob.retailer_sync_job_id).distinct(OnlineSyncJob.retailer_sync_job_id)
        new_sync_jobs = session.query(RetailerSyncJob).options(joinedload(RetailerSyncJob.retailer))\
            .filter(RetailerSyncJob.id.notin_(sub_query)).all()

        for new_sync_job in new_sync_jobs:
            OnlineSyncJob.create_or_updated_by(new_sync_job, new_sync_job.retailer, session)

        logging.info(u"查出变更的任务, 直接更新他")
        updated_online_sync_jobs = session.query(OnlineSyncJob)\
            .join(RetailerConfig, RetailerConfig.org_code == OnlineSyncJob.org_code)\
            .join(RetailerSyncJob, RetailerSyncJob.id == OnlineSyncJob.retailer_sync_job_id)\
            .filter(or_(
                RetailerConfig.updated_at != OnlineSyncJob.retailer_updated_at,
                RetailerSyncJob.updated_at != OnlineSyncJob.job_updated_at
            )).all()

        uniq_job_ids = list(set(map(lambda job: job.retailer_sync_job_id, updated_online_sync_jobs)))

        if len(uniq_job_ids) > 0:
            retailer_sync_jobs = session.query(RetailerSyncJob)\
                .options(joinedload(RetailerSyncJob.retailer))\
                .filter(RetailerSyncJob.id.in_(uniq_job_ids)).all()

            for updated_sync_job in retailer_sync_jobs:
                OnlineSyncJob.create_or_updated_by(updated_sync_job, updated_sync_job.retailer, session)

        retailer_configs = session.query(RetailerConfig).filter(RetailerConfig.database_migrated.is_(True)).all()
        for retailer_config in retailer_configs:
            RetailerSQL.sync_sqls(session, retailer_config)
Index('org_index', RetailerConfig.org_code)


class LivyStreamingTask(MaintenanceBase):
    __tablename__ = "livy_streaming_tasks"

    id = Column(Integer, autoincrement=True, nullable=False, primary_key=True)
    streaming_batch_id = Column(Integer, nullable=False)
    streaming_task_name = Column(VARCHAR(64))

    @staticmethod
    def record_livy_streaming_task(session, batch_id, task_name):
        #  type: (Session, int, str) -> None
        session.begin()
        task = LivyStreamingTask(streaming_batch_id=batch_id, streaming_task_name=task_name)
        session.add(task)
        session.commit()

    @staticmethod
    def remove_livy_streaming_task(session, batch_id):
        # type: (Session, int) -> None
        session.begin()
        session.query(LivyStreamingTask).filter(LivyStreamingTask.streaming_batch_id == batch_id)\
            .delete(synchronize_session=False)
        session.commit()

    @staticmethod
    def load_running_task(session, task_name):
        #  type: (Session, str) -> int
        task = session.query(LivyStreamingTask)\
            .filter(LivyStreamingTask.streaming_task_name == task_name).first()  # type: LivyStreamingTask
        if task is not None:
            return task.streaming_batch_id
        else:
            return None


class RetailerSyncJob(MaintenanceBase):
    """
    描述了这个商户的同步任务，一个同步任务被简单描述成一个基于DataX的sync job
    """
    __tablename__ = "retailer_sync_jobs"

    id = Column(Integer, autoincrement=True, nullable=False, primary_key=True)
    org_code = Column(String(32), ForeignKey("retailer_configs.org_code"), nullable=False)
    retailer = relationship("RetailerConfig", back_populates="retailer_sync_jobs")
    mode = Column(String(32), default="ss")
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    # 依赖上级任务的id
    depend_job_id = Column(Integer, nullable=True)
    name = Column(String(64), nullable=False)
    query_sql = Column(JSON)
    pre_sql = Column(JSON)
    target_tables = Column(JSON)
    target_columns = Column(JSON)
    session_sqls = Column(JSON)
    fetch_size = Column(Integer, nullable=False, default=1024, doc="同步数量")
    manual_create_json = Column(Boolean, default=False)
    task_priority = Column(Integer, nullable=False)
    """数据增量标示, 为null, 或者为空hash, 则标示该同步任务不做增量同步, 只做全量同步
    remote_field: 在query_sql中会插入一段类似 ${remote_field} > max_value
    local_field: 本地表中作为增量标示的字段, 会通过 select max(${local_field}) as max_value from target_tables[0]
    """
    increment_config = Column(JSON)

    def datax_config(self,
                     retailer_config: RetailerConfig,
                     config: dict,
                     query_sql: List[str]) -> dict:
        """
        根据当前数据库的配置, 生成datax的任务的json
        :return:
        """
        logging.info(retailer_config.database_config)
        sql_username = retailer_config.database_config['username']
        sql_password = retailer_config.database_config['password']
        sql_host = retailer_config.database_config['host']
        sql_port = retailer_config.database_config['port']
        sql_database = retailer_config.database_config['database']
        sql_jdbc_url = "jdbc:jtds:sqlserver://{0}:{1};DatabaseName={2}".format(sql_host, sql_port, sql_database)

        # 检测plugin name
        database_type = retailer_config.database_config.get('database_type')
        if database_type is None:
            database_type = 'sql_server'
        plugin_name = "sqlserverreader"
        if database_type == 'sybase':
            plugin_name = 'rdbmsreader'
            sql_jdbc_url = "jdbc:sybase:Tds:{0}:{1}?ServiceName={2}".format(
                sql_host, str(sql_port), sql_database
            )
        if database_type == 'mysql':
            plugin_name = 'mysqlreader'
            sql_jdbc_url = "jdbc:mysql://{0}:{1}/{2}".format(sql_host, str(sql_port), sql_database)
        if database_type == "oracle":
            plugin_name = "oraclereader"
            sid = retailer_config.database_config.get('sid')
            sql_jdbc_url = "jdbc:oracle:thin:@{0}:{1}:{2}".format(sql_host, str(sql_port), sid)
        jw_host = config["retailer"]["host"]
        jw_port = config["retailer"]["port"]

        jw_user = config["retailer"]["username"]
        jw_pass = config["retailer"]["password"]
        jw_jdbc_url = "jdbc:mysql://{0}:{1}/{2}?useUnicode=true&characterEncoding=utf-8" \
            .format(jw_host, jw_port, retailer_config.online_dw_name)

        write_mode = "insert"

        if self.increment_config is not None and len(self.increment_config.keys()) > 0:
            write_mode = "replace"

        job_config = {
            "timestamp": int(round(time.time() * 1000)),
            "job": {
                "setting": {"speed": {"byte": 1048576}},
                "content": [
                    {
                        "reader": {
                            "name": plugin_name,
                            "parameter": {
                                "username": sql_username,
                                "password": sql_password,
                                "connection": [
                                    {
                                        "querySql": query_sql,
                                        "jdbcUrl": [sql_jdbc_url]
                                    }
                                ],
                                "fetchSize": 1024
                            }
                        },
                        "writer": {
                            "name": "mysqlwriter",
                            "parameter": {
                                "writeMode": write_mode,
                                "username": jw_user,
                                "password": jw_pass,
                                "column": self.target_columns,
                                "session": self.session_sqls,
                                "preSql": self.pre_sql,
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
        return job_config

    def jw_datax_config(self,
                        retailer_config: RetailerConfig,
                        config: dict,
                        query_sql: List[str]) -> dict:
        """
        根据当前数据库的配置, 生成 mysql -> sql_server的datax的任务json
        :return:
        """

        logging.info("商户的数据库配置:")
        logging.info(retailer_config.database_config)

        retailer_username = retailer_config.database_config['username']
        retailer_password = retailer_config.database_config['password']
        retailer_host = retailer_config.database_config['host']
        retailer_port = retailer_config.database_config['port']
        retailer_database = retailer_config.database_config['database']
        retailer_jdbc_url = "jdbc:jtds:sqlserver://{0}:{1};DatabaseName={2}".format(retailer_host, retailer_port,
                                                                                    retailer_database)

        logging.info("Jw3336数据库配置:")
        jw_host = config["retailer"]["host"]
        jw_port = config["retailer"]["port"]

        jw_user = config["retailer"]["username"]
        jw_pass = config["retailer"]["password"]
        jw_jdbc_url = "jdbc:mysql://{0}:{1}/{2}?useUnicode=true&characterEncoding=utf-8" \
            .format(jw_host, jw_port, retailer_config.online_dw_name)

        job_config = {
            "timestamp": int(round(time.time() * 1000)),
            "job": {
                "setting": {"speed": {"byte": 1048576}},
                "content": [
                    {
                        "reader": {
                            "name": "mysqlreader",
                            "parameter": {
                                "username": jw_user,
                                "password": jw_pass,
                                "connection": [
                                    {
                                        "querySql": query_sql,
                                        "jdbcUrl": [jw_jdbc_url]
                                    }
                                ]
                            }
                        },
                        "writer": {
                            "name": "sqlserverwriter",
                            "parameter": {
                                "username": retailer_username,
                                "password": retailer_password,
                                "column": self.target_columns,
                                "preSql": self.pre_sql,
                                "connection": [
                                    {
                                        "jdbcUrl": retailer_jdbc_url,
                                        "table": self.target_tables
                                    }
                                ]
                            }
                        }
                    }
                ]
            }
        }
        return job_config

    def expanded_query_sql(self, maintenance_session: Session, database_config: dict) -> List[str]:
        """
        由于存在增量同步的sql, 因此, 需要做一次sql展开
        :return:
        """
        max_condition_pit = "1=1"
        if self.increment_config is not None and len(self.increment_config.keys()) > 0:
            remote_field = self.increment_config.get("remote_field", None)
            local_field = self.increment_config.get("local_field", None)

            if remote_field is not None and local_field is not None:
                table_name = self.target_tables[0]
                max_value_load_sql = """
                select max({0}) as max_value from {1}
                """.format(local_field, table_name)
                max_results = RetailerMySqlExecutor(
                    maintenance_session=maintenance_session,
                    database_config=database_config,
                    org_code=self.org_code,
                    sql=max_value_load_sql
                ).read()

                if len(list(max_results)) > 0 and len(list(max_results)[0]) > 0:
                    max_value = list(max_results)[0][0]['max_value']
                    if max_value is not None:
                        max_condition_pit = "{0} > '{1}'".format(remote_field, str(max_value))

        result_sqls = []
        for one_query_sql in self.query_sql:
            result_sqls.append(Template(one_query_sql).render(max_condition_pit=max_condition_pit))
        return result_sqls
Index('org_index', RetailerSyncJob.org_code)


class OnlineSyncJob(MaintenanceBase):
    """
    这个模型管理了对应的商户同步任务模型, 这个模型存在两个点可能会变更它:

    1. RetailerConfig发生变更
    2. RetailerSyncJob中对应的job发生变更, 或者删除, 这个时候回重新创建任务
    """

    __tablename__ = "online_sync_jobs"

    id = Column(Integer, autoincrement=True, nullable=False, primary_key=True)
    org_code = Column(String(32), nullable=False)
    mode = Column(String(32), default="ss")
    job_name = Column(String(128), nullable=False)
    json_config = Column(JSON)
    retailer_sync_job_id = Column(Integer, nullable=False)
    retailer_updated_at = Column(DateTime, nullable=False)
    job_updated_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    created_at = Column(DateTime, nullable=False)
    task_priority = Column(Integer, nullable=False)

    @staticmethod
    def clear_useless_jobs(session: Session):
        """

        :param session: Session
        :return:
        """
        retailer_sync_job_ids = session.query(RetailerSyncJob.id).all()

        """
        清理多余的OnlineSyncJob
        """
        session.query(OnlineSyncJob) \
            .filter(~OnlineSyncJob.retailer_sync_job_id.in_(retailer_sync_job_ids)).delete()

    @staticmethod
    def create_or_updated_by(retailer_online_job, retailer_config, session):
        # type: (RetailerSyncJob, RetailerConfig, Session) -> OnlineSyncJob
        session.begin()
        online_sync_job = session.query(OnlineSyncJob) \
            .filter(OnlineSyncJob.retailer_sync_job_id == retailer_online_job.id).first()
        """:type:OnlineSyncJob"""
        if online_sync_job is None:
            online_sync_job = OnlineSyncJob()
            online_sync_job.retailer_sync_job_id = retailer_online_job.id
            online_sync_job.job_name = retailer_online_job.name
            online_sync_job.org_code = retailer_online_job.org_code
            online_sync_job.created_at = datetime.datetime.now()
            online_sync_job.task_priority = retailer_online_job.task_priority
            session.add(online_sync_job)

        online_sync_job.update_by(retailer_online_job, retailer_config, session)
        session.commit()
        return online_sync_job

    def update_by(self, retailer_online_job, retailer_config, session):
        # type: (RetailerSyncJob, RetailerConfig, Session) -> OnlineSyncJob

        self.retailer_updated_at = retailer_config.updated_at
        self.job_updated_at = retailer_online_job.updated_at
        self.job_name = retailer_online_job.name
        self.mode = retailer_online_job.mode
        self.task_priority = retailer_online_job.task_priority
        self.updated_at = datetime.datetime.now()

        """
        retailer config 中sqlserver的数据库密码的格式应该是这个样子:

        "username" : "sa",
        "password" : "lebja2333",
        "host" : "222.213.163.236",
        "port" : 33411,
        "database" : "wuerp_bas",
        "timeout" : 10,
        "login_timeout" : 4    
        """
        logging.info(retailer_config.database_config)
        sql_username = retailer_config.database_config['username']
        sql_password = retailer_config.database_config['password']
        sql_host = retailer_config.database_config['host']
        sql_port = retailer_config.database_config['port']
        sql_database = retailer_config.database_config['database']
        sql_jdbc_url = "jdbc:jtds:sqlserver://{0}:{1};DatabaseName={2}".format(sql_host, sql_port, sql_database)

        # 检测plugin name
        database_type = retailer_config.database_config.get('database_type')
        if database_type is None:
            database_type = 'sql_server'
        plugin_name = "sqlserverreader"

        if database_type == 'sybase':
            plugin_name = 'rdbmsreader'
            sql_jdbc_url = "jdbc:sybase:Tds:{0}:{1}?ServiceName={2}".format(
                sql_host, str(sql_port), sql_database
            )

        if database_type == 'mysql':
            plugin_name = 'mysqlreader'
            sql_jdbc_url = "jdbc:mysql://{0}:{1}/{2}".format(sql_host, str(sql_port), sql_database)

        if database_type == "oracle":
            plugin_name = "oraclereader"
            sid = retailer_config.database_config.get('sid')
            sql_jdbc_url = "jdbc:oracle:thin:@{0}:{1}:{2}".format(sql_host, str(sql_port), sid)

        config = DatabaseConfig.current_config(session)
        jw_host = config["retailer"]["host"]
        jw_port = config["retailer"]["port"]

        jw_user = config["retailer"]["username"]
        jw_pass = config["retailer"]["password"]
        jw_jdbc_url = "jdbc:mysql://{0}:{1}/{2}?useUnicode=true&characterEncoding=utf-8" \
            .format(jw_host, jw_port, retailer_config.online_dw_name)
        if retailer_online_job.manual_create_json is False:
            new_json = {
                "timestamp": int(round(time.time() * 1000)),
                "job": {
                    "setting": {"speed": {"byte": 1048576}},
                    "content": [
                        {
                            "reader": {
                                "name": plugin_name,
                                "parameter": {
                                    "username": sql_username,
                                    "password": sql_password,
                                    "connection": [
                                        {
                                            "querySql": retailer_online_job.query_sql,
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
                                    "column": retailer_online_job.target_columns,
                                    "session": retailer_online_job.session_sqls,
                                    "preSql": retailer_online_job.pre_sql,
                                    "connection": [
                                        {
                                            "jdbcUrl": jw_jdbc_url,
                                            "table": retailer_online_job.target_tables
                                        }
                                    ]
                                }
                            }
                        }
                    ]
                }
            }
            # 这里是为了修复线上一个bug
            # 在python的这套sql机制中, json和其它字段是分开在两个sql中更新
            # 如果json没有变化, 那么, 就会出现这个报错:
            # UPDATE statement on table 'online_sync_jobs' expected to update 1 row(s); 0 were matched.
            if json.dumps(self.json_config) != json.dumps(new_json):
                logging.info(json.dumps(self.json_config, sort_keys=True))
                logging.info(json.dumps(new_json, sort_keys=True))
                logging.info("json不一样")
                self.json_config = new_json

        return self
Index("org_code", OnlineSyncJob.org_code)


class RetailerSQL(MaintenanceBase):
    """
    每个商户都有一些独有的存储过程, 这些存储过程会存放在这个数据库里面, 并被同步到正确的网络
    """

    __tablename__ = 'retailer_sqls'

    id = Column(Integer, autoincrement=True, nullable=False, primary_key=True)
    org_code = Column(String(32), ForeignKey("retailer_configs.org_code"), nullable=False)
    retailer = relationship("RetailerConfig", back_populates="retailer_sqls")
    file_name = Column(VARCHAR(length=64), nullable=False)
    sql_content = Column(TEXT(), default="", server_default="", nullable=False)
    sql_sha1 = Column(VARCHAR(128))

    @staticmethod
    def sync_sqls(session, retailer_config):
        # type: (Session, RetailerConfig) -> None
        retailer_sqls = retailer_config.retailer_sqls  # type: List[RetailerSQL]
        current_online_sqls = \
            session.query(OnlineSQL).filter(OnlineSQL.org_code == retailer_config.org_code).all()
        grouped_online_sqls = {}
        for online_sql in current_online_sqls:  # type: OnlineSQL
            grouped_online_sqls[online_sql.file_name] = online_sql

        need_update_sql = []
        for retailer_sql in retailer_sqls:  # type: RetailerSQL
            online_sql = grouped_online_sqls.get(retailer_sql.file_name, None)  # type: OnlineSQL
            if online_sql is None:
                need_update_sql.append(retailer_sql)
            elif online_sql.sql_sha1 != retailer_sql.sql_sha1:
                need_update_sql.append(retailer_sql)

        config = DatabaseConfig.current_config(session)
        jw_host = config["retailer"]["host"]
        jw_port = config["retailer"]["port"]

        jw_user = config["retailer"]["username"]
        jw_pass = config["retailer"]["password"]
        mysql_connection = MySQLdb.connect(host=jw_host, port=int(jw_port), user=jw_user,
                                           passwd=jw_pass, db=retailer_config.online_dw_name, charset="utf8",
                                           use_unicode=True)
        for retailer_sql in need_update_sql:  # type: RetailerSQL
            mysql_connection.query(retailer_sql.sql_content)
            while mysql_connection.next_result() > 0:
                mysql_connection.store_result()

            retailer_sql.update_online_sql(session)

    def update_online_sql(self, session):
        # type: (Session) -> None
        session.begin()
        online_sql = session.query(OnlineSQL).filter(OnlineSQL.org_code == self.org_code)\
            .filter(OnlineSQL.file_name == self.file_name).first()

        if online_sql is None:
            online_sql = OnlineSQL(org_code=self.org_code, file_name=self.file_name,
                                   sql_content=self.sql_content, sql_sha1=self.sql_sha1
                                   )
            session.add(online_sql)
        else:
            online_sql.sql_sha1 = self.sql_sha1
            online_sql.sql_content = self.sql_content

        session.commit()


class OnlineSQL(MaintenanceBase):
    """
    一个商户在线上的存储过程
    """

    __tablename__ = 'online_sqls'

    id = Column(Integer, autoincrement=True, nullable=False, primary_key=True)
    org_code = Column(String(32), nullable=False)
    file_name = Column(VARCHAR(length=64), nullable=False)
    sql_content = Column(TEXT(), default="", server_default="", nullable=False)
    sql_sha1 = Column(VARCHAR(128))




if __name__ == "__main__":
    databaseconfig = DatabaseConfig()
    session = BaseService.initialize_maintenance_objects()
    config = databaseconfig.current_config(session)
    print(config)

