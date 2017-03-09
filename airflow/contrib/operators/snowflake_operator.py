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

import logging

from airflow.contrib.hooks import SnowflakeHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class SnowflakeOperator(BaseOperator):
    """
    Executes sql code in a Snowflake database & warehouse

    :param postgres_conn_id: reference to a specific postgres database
    :type postgres_conn_id: string
    :param sql: the sql code to be executed
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    :param database: name of database which overwrite defined one in connection
    :type database: string
    :param database: name of database which overwrite defined one in connection
    :type database: string
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
        self.postgres_conn_id = snowflake_conn_id
        self.autocommit = autocommit
        self.parameters = parameters
        self.database = database
        self.warehouse = warehouse

    def execute(self, context):
        # Attempt to get warehouse & database -- operator first, then
        # connection.
        self.hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)

        database = self.database or self.hook.extra_params.get('database')
        warehouse = self.warehouse or self.hook.extra_params.get('warehouse')

        # Use array of statements to make sure they are run on the same connection
        # (as opposed to passing to hook individually)
        sqls = [self.sql]
        if database:
            sqls = ('use database ' + database) + sqls
        if warehouse:
            sqls = ('use warehouse ' + database) + sqls

        logging.info('Executing: ' + str(sqls))
        self.hook.run(sqls, self.autocommit, parameters=self.parameters)
