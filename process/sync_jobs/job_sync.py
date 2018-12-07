import os
import json
import hashlib
from datetime import datetime
from os import DirEntry
from os.path import join, basename
from typing import List, Dict
import pymysql


def process_dir_name(path):
    # type: (str) -> None
    if os.path.isfile(join(path, "config_localhost.json")):
        EnvSyncDirectory(path).sync()
    else:
        print(u"环境{0}找不到数据的配置文件[config.json]".format(basename(path)))



class EnvSyncDirectory(object):
    def __init__(self, path):
        # type: (str) -> EnvSyncDirectory
        self.path = path
        self.config = None
        self.connection = None
        self.tasks = []

    def sync(self):
        self._init_config()
        self._init_connection()
        print(u"初始化环境{0}的配置: {1}".format(basename(self.path), self.config))
        self._scan_orgs()

    def _init_config(self):
        with open(join(self.path, 'config_localhost.json'), 'r', encoding="utf-8") as config_content:
            self.config = json.load(config_content)

    def _init_connection(self):
        self.connection = pymysql.connect(host=self.config['host'], port=self.config["port"],
                                          user=self.config["username"], password=self.config["password"],
                                          database=self.config["database_name"], charset="utf8")

    def _scan_orgs(self):
        self.tasks.clear()
        for org_dir in os.scandir(self.path):
            dir_start_with_dot = entry.name.startswith(".")
            if all([org_dir.is_dir(), not dir_start_with_dot]):
                org_code = basename(org_dir.name)
                if os.path.isdir(join(self.path, org_dir.name, "dr")):
                    dr_tasks = TaskScanner(org_code=org_code, mode="dr",
                                           path=join(self.path, org_dir.name, "dr")).scan_tasks(self.connection)
                    self.tasks.extend(dr_tasks)
                if os.path.isdir(join(self.path, org_dir.name, "stock")):
                    ss_tasks = TaskScanner(org_code=org_code, mode="stock",
                                           path=join(self.path, org_dir.name, "stock")).scan_tasks(self.connection)
                    self.tasks.extend(ss_tasks)

                if os.path.isdir(join(self.path, org_dir.name, "ss")):
                    ss_tasks = TaskScanner(org_code=org_code, mode="ss",
                                           path=join(self.path, org_dir.name, "ss")).scan_tasks(self.connection)
                    self.tasks.extend(ss_tasks)
                if os.path.isdir(join(self.path, org_dir.name, "plan")):
                    ss_tasks = TaskScanner(org_code=org_code, mode="plan",
                                           path=join(self.path, org_dir.name, "plan")).scan_tasks(self.connection)
                    self.tasks.extend(ss_tasks)



class TaskScanner(object):
    def __init__(self, org_code, mode, path):
        self.org_code = org_code
        self.mode = mode
        self.path = path

    def scan_tasks(self, connection):
        tasks = []
        for task_dir in os.scandir(self.path):
            dir_start_with_dot = task_dir.name.startswith(".")
            if all([task_dir.is_dir(), not dir_start_with_dot]):
                task = Task(path=join(self.path, task_dir.name), org_code=self.org_code, mode=self.mode)
                tasks.append(task)

        self._update_task_with_connection(tasks, connection)
        return tasks

    def _update_task_with_connection(self, tasks, connection):
        grouped_tasks = dict()
        for task in tasks:
            grouped_tasks[task.task_name] = task

        if len(tasks) == 0:
            just_clear_sql = """
                delete from retailer_sync_jobs
                where retailer_sync_jobs.org_code = %scale 
                and retailer_sync_jobs.mode = %s
            """
            connection.begin()
            cursor = connection.cursor()
            cursor.execute(just_clear_sql, [self.org_code, self.mode])
            connection.commit()
            return

        all_task_names = list(map(lambda task: task.task_name, tasks))
        print(all_task_names)
        connection.begin()
        cursor = connection.cursor()

        cursor.execute("select * from retailer_configs where org_code = %s", [self.org_code])
        data = cursor.fetchall()
        if len(data) == 0:
            insert_org_sql = """
                insert into `retailer_configs` (`org_code`,`database_type`,`database_config`,`created_at`,`updated_at`,`activated`,`online_dw_name`)
                values(%s, 'sql_server', %s, %s, %s, 0, %s);
            """
            cursor.execute(insert_org_sql, [self.org_code, json.dumps(dict()), datetime.now(), datetime.now(), "{0}DW".format(self.org_code)])

        clear_sql = """
            delete from retailer_sync_jobs
            where retailer_sync_jobs.org_code = %s
            and retailer_sync_jobs.name not in ({0})
            and retailer_sync_jobs.mode = %s
        """.format(', '.join(list(map(lambda x:'%s', all_task_names))))
        query_args = [self.org_code]
        query_args.extend(all_task_names)
        query_args.extend([self.mode])
        cursor.execute(clear_sql, query_args)
        data = cursor.fetchall()

        query_existed_sql = """
SELECT * FROM retailer_sync_jobs 
WHERE retailer_sync_jobs.org_code = %s
  AND retailer_sync_jobs.name IN ({0})
  AND retailer_sync_jobs.mode = %s
        """.format(', '.join(list(map(lambda x: '%s', all_task_names))))

        update_sql = """
UPDATE retailer_sync_jobs SET
  retailer_sync_jobs.updated_at = %s, 
  retailer_sync_jobs.query_sql = %s,
  retailer_sync_jobs.pre_sql = %s,
  retailer_sync_jobs.target_tables = %s,
  retailer_sync_jobs.target_columns = %s,
  retailer_sync_jobs.fetch_size = %s,
  retailer_sync_jobs.session_sqls = %s,
  retailer_sync_jobs.task_priority = %s,
  retailer_sync_jobs.increment_config = %s,
  retailer_sync_jobs.preferred_db_config_name = %s
WHERE retailer_sync_jobs.id = %s
        """
        cursor.execute(query_existed_sql, query_args)
        data = cursor.fetchall()


        if len(data) > 0:
            print(data)

        for database_task in data:
            task_id = database_task[0]
            task_name = database_task[5]

            task = grouped_tasks.get(task_name)
            update_args = [
                datetime.now(),
                json.dumps(task.query_sqls, indent=4),
                json.dumps(task.pre_sqls, indent=4),
                json.dumps(task.target_tables, indent=4),
                json.dumps(task.target_columns, indent=4),
                task.fetch_size,
                json.dumps(task.session_sqls, indent=4),
                json.dumps(task.task_priority, indent=4),
                json.dumps(task.increment_config, indent=4),
                task.preferred_db_config_name,
                task_id
            ]
            cursor.execute(update_sql, update_args)
            grouped_tasks.pop(task_name, None)

        insert_sql = """
INSERT INTO `retailer_sync_jobs` (
  `org_code`, `created_at`, `updated_at`, `name`, 
  `query_sql`, `pre_sql`, `target_tables`, `target_columns`, 
  `manual_create_json`, `fetch_size`, `session_sqls`, `mode`, `task_priority`, `increment_config`, `preferred_db_config_name`)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        for task_name in grouped_tasks.keys():

            task = grouped_tasks[task_name]
            insert_args = [
                task.org_code,
                datetime.now(),
                datetime.now(),
                task.task_name,
                json.dumps(task.query_sqls, indent=4),
                json.dumps(task.pre_sqls, indent=4),
                json.dumps(task.target_tables, indent=4),
                json.dumps(task.target_columns, indent=4),
                False,
                task.fetch_size,
                json.dumps(task.session_sqls, indent=4),
                task.mode,
                json.dumps(task.task_priority, indent=4),
                json.dumps(task.increment_config, indent=4),
                task.preferred_db_config_name
            ]
            cursor.execute(insert_sql, insert_args)

        connection.commit()



class Task(object):
    def __init__(self, path, org_code, mode):
        self.path = path
        self.org_code = org_code
        self.mode = mode
        self.task_name = basename(self.path)
        self.fetch_size = 0
        self.task_priority = 0
        self.target_columns = []
        self.target_tables = []
        self.pre_sqls = []
        self.session_sqls = []
        self.query_sqls = []
        self.increment_config = []
        self.preferred_db_config_name = None

        self._load_meta()
        self._load_pre_sqls()
        self._load_query_sqls()
        self._load_session_sqls()

    def _load_meta(self):
        with open(join(self.path, "meta.json"), "r", encoding="utf-8") as meta_config:
            meta_config = json.load(meta_config)
            self.target_columns = meta_config.get("target_columns", [])
            self.target_tables = meta_config.get("target_tables", [])
            self.fetch_size = meta_config.get("fetch_size", 1024)
            self.task_priority = meta_config.get("task_priority", 0)
            self.increment_config = meta_config.get("increment_config", {})
            self.preferred_db_config_name = meta_config.get("preferred_db_config_name", "default")

    def _load_pre_sqls(self):
        self.pre_sqls.extend(self._load_sqls("pre_sql"))

    def _load_query_sqls(self):
        self.query_sqls.extend(self._load_sqls("query_sql"))

    def _load_session_sqls(self):
        self.session_sqls.extend(self._load_sqls("session_sql"))

    def _load_sqls(self, prefix_name):
        all_sqls = []
        for sql_file in os.scandir(self.path):
            sql_with_prefix = sql_file.name.startswith(prefix_name + ".")
            file_end_with_sql = sql_file.name.endswith(".sql")

            if sql_with_prefix and file_end_with_sql:
                content = open(join(self.path, sql_file.name), "r", encoding="utf-8").read()
                index = sql_file.name.split(".")[1]
                all_sqls.append([content, index])

        all_sqls = sorted(all_sqls, key=lambda sql: sql[1])
        all_sql_contents = []
        for c in all_sqls:
            all_sql_contents.append(c[0])
        return all_sql_contents




dir_path = os.path.dirname(os.path.realpath(__file__))
for entry in os.scandir("."):
    start_with_dot = entry.name.startswith(".")
    if all([entry.is_dir(), not start_with_dot]):
        print(entry.name)
        process_dir_name(join(dir_path, entry.name))