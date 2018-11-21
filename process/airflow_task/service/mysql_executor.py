import logging
from typing import Union, Iterable, Any, List

import MySQLdb
from MySQLdb.cursors import DictCursor
from sqlalchemy.orm import Session


class JwMysqlExecutor(object):
    def __init__(self,
                 maintenance_session: Session,
                 database_config: dict,
                 sql: Union[str, Iterable[str]],
                 data_source_name: str = 'mysql_default',
                 parameters: dict = None,
                 autocommit: bool = False,
                 database: str = None) -> None:
        super(JwMysqlExecutor, self).__init__()
        self.maintenance_session = maintenance_session
        self.database_config = database_config
        self.sql = sql
        self.data_source_name = data_source_name
        self.parameters = parameters
        self.autocommit = autocommit
        self.database = database
        self.results = []

    def execute(self, cursor_klass=None):
        logging.info('Executing: ' + str(self.sql))
        conn = self.connection()
        if isinstance(self.sql, str):
            self.sql = [self.sql]

        if self.autocommit is True:
            conn.autocommit(self.autocommit)
        if cursor_klass is None:
            cur = conn.cursor()
        else:
            cur = conn.cursor(cursor_klass)

        for s in self.sql:
            logging.info(s)
            if self.parameters is not None:
                cur.execute(s, self.parameters)
            else:
                cur.execute(s)
            result = cur.fetchall()
            self.results.append(result)

        cur.close()
        conn.commit()
        conn.close()

    def read(self) -> Iterable[Any]:
        self.execute(cursor_klass=DictCursor)
        return self.results

    def connection(self):
        full_config = self.database_config[self.data_source_name]  # type: dict
        host = full_config['host']
        port = full_config['port']
        username = full_config['username']
        password = full_config['password']

        if 'database_name' in full_config.keys():
            self.database = full_config['database_name']

        return MySQLdb.connect(host=host, port=port, user=username, passwd=password, db=self.database, charset="utf8")


class RetailerMySqlExecutor(JwMysqlExecutor):
    def __init__(self, org_code, *args, **kwargs):
        kwargs['database'] = None
        kwargs['data_source_name'] = 'retailer'
        super(RetailerMySqlExecutor, self).__init__(*args, **kwargs)
        self.org_code = org_code

    def connection(self):
        full_config = self.database_config[self.data_source_name] # type: dict
        host = full_config['host']
        port = full_config['port']
        username = full_config['username']
        password = full_config['password']

        from process.model.retailer_config import RetailerConfig
        self.database = RetailerConfig.dw_name(self.org_code, self.maintenance_session)

        return MySQLdb.connect(host=host, port=port, user=username, passwd=password, db=self.database, charset="utf8")