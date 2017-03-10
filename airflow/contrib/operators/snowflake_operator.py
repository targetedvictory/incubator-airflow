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

import logging

from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class SnowflakeOperator(BaseOperator):
    """
    Executes sql code in a Snowflake database & warehouse

    :param sql: the sql code to be executed
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    :param snowflake_conn_id: reference to snowflake connection definiton
    :type snowflake_conn_id: string
    :param database: optional database overriding one in connection
    :type database: string
    :param warehouse: optional warehouse overriding one in connection
    :type warehouse: string
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self, sql,
            snowflake_conn_id='snowflake_default',
            database=None,
            warehouse=None,
            autocommit=False,
            parameters=None,
            *args, **kwargs):
        super(SnowflakeOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.snowflake_conn_id = snowflake_conn_id
        self.autocommit = autocommit
        self.parameters = parameters
        self.database = database
        self.warehouse = warehouse

    def execute(self, context):
        self.hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id,
            database=self.database, warehouse=self.warehouse)
        logging.info('Executing: ' + str(self.sql))
        self.hook.run(self.sql, self.autocommit, parameters=self.parameters)
