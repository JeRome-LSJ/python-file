#!/usr/bin/env python
# -*- coding: utf-8 -*-
# hive_executor.py
#


import time
import logging

import spark5k

_logger = logging.getLogger('abs_mock')
_logger.setLevel(logging.INFO)


def run_hql_map(sql_map):
    base_task = spark5k.get_base_sql_task()
    sql_task = base_task.SqlTask()
    sql_task.set_sql_runner(base_task.RUNNER_HIVE)
    sql_task.set_customized_items({})

    time_start = time.time()
    sql_task.execute_sqls(sql_map)
    _logger.info('hql执行完毕，耗时 %d sec.', (time.time() - time_start))


def run_hql(sql):
    base_task = spark5k.get_base_sql_task()
    sql_task = base_task.SqlTask()
    sql_task.set_sql_runner(base_task.RUNNER_HIVE)
    sql_task.set_customized_items({})

    time_start = time.time()
    sql_task.execute_sqls({"sql_01": sql})
    _logger.info('hql执行完毕，耗时 %d sec.', (time.time() - time_start))
