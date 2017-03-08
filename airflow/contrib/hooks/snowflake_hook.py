import logging

import snowflake.connector
from airflow.hooks import DbApiHook


class SnowflakeHook(DbApiHook):
    conn_name_attr = 'snowflake_conn_id'
    default_conn_name = 'snowflake_default'
    supports_autocommit = True
    connector = snowflake.connector

    def __init__(self, *args, **kwargs):
        super(SnowflakeHook, self).__init__(*args, **kwargs)

    def get_conn(self):
        """Returns a connection object
        """
        db = self.get_connection(getattr(self, self.conn_name_attr))
        self.extra_params = db.extra_dejson
        return self.connector.connect(
            user=db.login,
            account=self.extra_params['account'],
            password=db.password)

    def set_autocommit(self, conn, autocommit):
        conn.autocommit(autocommit)
