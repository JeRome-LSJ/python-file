#!/usr/bin/env python
# -*- coding: utf-8 -*-
# spark_executor.py
#

import logging
import time
import pandas as pd

_logger = logging.getLogger('abs_mock')
_logger.setLevel(logging.INFO)


def execute_hql(spark_session, hql):
    _logger.info("开始执行hql")
    _logger.info(hql)
    time_start = time.time()
    spark_session.sql(hql).show()
    _logger.info('hql执行完毕，耗时 %d ms.', (time.time() - time_start))


def execute_hql_rlt(spark_session, hql):
    _logger.info("开始执行hql")
    _logger.info(hql)
    time_start = time.time()
    rlt_df = spark_session.sql(hql).toPandas()
    _logger.info('hql执行完毕，耗时 %d ms.', (time.time() - time_start))
    _logger.info(rlt_df)
    return rlt_df


def execute_insert(spark_session, data, columns, table):
    _logger.info("开始执行Spark插入数据")
    _logger.info(data)
    _logger.info(columns)
    _logger.info(table)
    spark_session.createDataFrame(pd.DataFrame(data, columns=columns)).repartition(4).write.format("hive").insertInto(table, overwrite=False)
    _logger.info("Spark插入数据执行结束")
