# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Initial contributors:
# andrew@snowflake.net
# dabercrombie@sharethrough.com
# psheridan@crossscreen.media

import snowflake.connector
from airflow.hooks import DbApiHook

class SnowflakeHook(DbApiHook):
    conn_name_attr = 'snowflake_conn_id'
    default_conn_name = 'snowflake_default'
    supports_autocommit = True
    connector = snowflake.connector

    def __init__(self, database=None, warehouse=None, *args, **kwargs):
        super(SnowflakeHook, self).__init__(*args, **kwargs)
        db = self.get_connection(getattr(self, self.conn_name_attr))
        # To set in connection, set this in extra field:
        # {"database":"my_db","warehouse":"my_wh"}
        self.extra_params = db.extra_dejson
        self.database = database or self.extra_params.get('database')
        self.warehouse = warehouse or self.extra_params.get('warehouse')

    def get_conn(self):
        """ Returns a connection object
        """
        db = self.get_connection(getattr(self, self.conn_name_attr))
        # Note mapping of the host field in the connection object to
        # the Snowflake account.  database and warehouse are available
        # in the extra_params dict. Apply them in this method
        # because it's used internally by other base class methods.
        conn = self.connector.connect(
            user=db.login,
            account=db.host,
            password=db.password)

        if self.database or self.warehouse:
            cur = conn.cursor()
            if self.database:
                cur.execute('use database ' + self.database)
            if self.warehouse:
                cur.execute('use warehouse ' + self.warehouse)
            cur.close()
        return conn

    def execute_string(self, sql):
        """ This is a wrapper for Snowflake's Connection.execute_string
            method, which can execute multiple semicolon-separated
            SQL statements (unlike Cursor.execute, which cannot).
        """
        # Note this uses defaults for remove comments/return cursors
        self.get_conn().execute_string(sql)
