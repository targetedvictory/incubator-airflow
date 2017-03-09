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


import snowflake.connector
from airflow.hooks import DbApiHook

class SnowflakeHook(DbApiHook):
    conn_name_attr = 'snowflake_conn_id'
    default_conn_name = 'snowflake_default'
    supports_autocommit = True
    connector = snowflake.connector

    def __init__(self, *args, **kwargs):
        super(SnowflakeHook, self).__init__(*args, **kwargs)
        db = self.get_connection(getattr(self, self.conn_name_attr))
        self.extra_params = db.extra_dejson

    def get_conn(self):
        """Returns a connection object
        """
        db = self.get_connection(getattr(self, self.conn_name_attr))
        # Note mapping of the host field in the connection object to
        # the Snowflake account.  database and warehouse are available
        # in the extra_params dict -- don't apply them here in case
        # they are overridden in some cases.
        return self.connector.connect(
            user=db.login,
            account=db.host,
            password=db.password)
