#!/usr/bin/env python
# -*- coding: utf-8 -*-
# spark5k.py
#

from __future__ import unicode_literals
from py_compat import *

import os
import sys
import csv
import json
import base64
import datetime
import logging
import pprint
import traceback

reload(logging)
LOG_FMT = "%(levelname)s:[%(threadName)s] %(module)s.%(funcName)s[:%(lineno)d] >>> %(message)s <<<"
logging.basicConfig(level=logging.INFO, format=LOG_FMT, datefmt='%m%d %H:%M:%S')

logging.warning('sys.version_info:%s', sys.version_info)
logging.warning('sys.path:%s', pprint.pformat(sys.path))
logging.warning('sys.argv:%s', pprint.pformat(sys.argv))

import psutil
import numpy as np
import pandas as pd

print('numpy:', np.__version__, 'pandas:', pd.__version__)

import pyspark
from pyspark.sql import *
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

# from pyspark.sql.types import *
# import pyspark.sql.functions as fn

print('pyspark:', pyspark.__version__)
spark_conf = None  # type: SparkConf
spark_context = None  # type: SparkContext
spark_session = None  # type: SparkSession
'''################'''
_logger = logging.getLogger('spark5k')
_logger.setLevel(logging.INFO)

UTF_8 = 'UTF-8'
NULL_STR = 'null'
EMPTY_STR = ''
MAX_TIMEOUT = 60 * 60  # 60 min
# 进程数量限制
MAX_PROC = min(24, max(6, int(0.5 * psutil.cpu_count())))
_logger.info('cpu_count:%d, max_proc:%d', psutil.cpu_count(), MAX_PROC)

job_params = None
run_date = datetime.datetime.now()  # type: datetime.datetime
job_date = (run_date - datetime.timedelta(1)).date()  # type: datetime.date
work_in = os.getcwd()
_logger.info('run_date:%s, work_in:%s', run_date, work_in)
_boot_dir = os.path.split(os.path.realpath(__file__))[0]
'''################'''


def init_args(args=None):
    _logger.info('init_args:%s', pprint.pformat(args))
    global job_date
    global job_params
    job_params = get_galaxy_job_params()
    if job_params:
        _logger.warning('job_params:%s', pprint.pformat(job_params))
        txDate = job_params["sys"]["core"]["txDate"]
        job_date = datetime.datetime.strptime(txDate, "%Y-%m-%d").date()
    elif len(args) >= 1:
        job_date = datetime.datetime.strptime(args[1], "%Y-%m-%d").date()
    _logger.warning('job_date:%s', job_date)

    memory = psutil.virtual_memory()
    _logger.info('memory:%s', memory)
    total_m = round((float(memory.total) / 1024 / 1024), 2)  # 总内存MB
    used_m = round((float(memory.used) / 1024 / 1024), 2)  # 已用内存MB
    free_m = round((float(memory.free) / 1024 / 1024), 2)  # 空闲内存MB
    avlb_m = round((float(memory.available) / 1024 / 1024), 2)  # 可用内存MB
    used_total = round((float(memory.used) / float(memory.total) * 100), 2)  # 内存使用率%
    free_total = round((float(memory.free) / float(memory.total) * 100), 2)  # 内存空闲率%
    avlb_total = round((float(memory.available) / float(memory.total) * 100), 2)  # 内存可用率%
    _logger.info('total_m:%d, used_m:%d, free_m:%d, avlb_m:%d, used_total:%d, free_total:%d, avlb_total:%d',
                 total_m, used_m, free_m, avlb_m, used_total, free_total, avlb_total)


def _map_to_pandas(rdds):
    # https://cf.jd.com/pages/viewpage.action?pageId=174836288
    return [pd.DataFrame(list(rdds))]


def to_pandas(df, n_partitions=None):
    # 分布式的 toPandas
    if n_partitions is not None:
        df = df.repartition(n_partitions)
    df_pand = df.rdd.mapPartitions(_map_to_pandas).collect()
    df_pand = pd.concat(df_pand)
    df_pand.columns = df.columns
    return df_pand


def init_spark_session(app_name='spark5k.py'):
    global spark_conf
    global spark_context
    global spark_session

    # spark.driver.extraJavaOptions
    spark_conf = SparkConf()
    spark_conf.setAppName(app_name) \
        .set("spark.sql.catalogImplementation", "hive") \
        .set('spark.executor.memory', '8g') \
        .set('spark.driver.memory', '8g') \
        .set('spark.driver.maxResultsSize', '0') \
        .set('spark.driver.extraJavaOptions', ' -Dfile.encoding=UTF-8 ') \
        .set('spark.executor.extraJavaOptions', ' -Dfile.encoding=UTF-8 ') \
        .set('spark.scheduler.listenerbus.eventqueue.capacity', '80000') \
        .set("hive.cli.print.header", "true") \
        .set("hive.exec.orc.split.strategy", "ETL") \
        .set("hive.exec.dynamici.partition", "true") \
        .set("hive.exec.dynamic.partition.mode", "nonstrict")

    spark_context = SparkContext.getOrCreate(spark_conf)
    spark_context.setLogLevel("WARN")
    quiet_spark_logs(spark_context)

    spark_session = SparkSession(spark_context)
    set_spark_var(spark_session, job_date)
    return spark_session


def get_galaxy_job_params():
    base_params = os.environ.get('galaxy_job_params')
    if base_params is None:
        logging.error('作业参数为空, 请确认环境变量galaxy_job_params中是否有值')
        return None

    return json.loads(base64_decode_str(base_params))


def get_base_sql_task():
    from template import base_sql_task
    return base_sql_task


def get_galaxy_times(tx_date=None):
    # type: (str) -> dict
    # 银河的时间变量
    if tx_date is None:
        tx_date = job_date.strftime('%Y-%m-%d')
    txVars = {'TX_DATE': tx_date}
    try:
        base_task = get_base_sql_task()
        time_dict = base_task.get_times(tx_date, None)
        for tx in time_dict:
            if time_dict[tx]:
                txVars[tx] = time_dict[tx]
    except Exception as _e:
        logging.error(_e)
        _logger.error(traceback.format_exc())
    return txVars


def get_date_var(data_date=None):
    if data_date is None:
        data_date = job_date

    dateVars = {}
    day_0_ago = data_date + datetime.timedelta(1)
    dateVars['day_0_later8'] = day_0_ago.strftime('%Y%m%d')
    dateVars['day_0_later10'] = day_0_ago.strftime('%Y-%m-%d')
    day_1_later = data_date + datetime.timedelta(2)
    dateVars['day_1_later8'] = day_1_later.strftime('%Y%m%d')
    dateVars['day_1_later10'] = day_1_later.strftime('%Y-%m-%d')

    for of in range(0, 8):
        day = day_0_ago - datetime.timedelta(of)
        dateVars['day_{}_ago8'.format(of)] = day.strftime('%Y%m%d')
        dateVars['day_{}_ago10'.format(of)] = day.strftime('%Y-%m-%d')

    _logger.info('data_date=%s, dateVars=%s', data_date, dateVars)
    return dateVars


def set_spark_var(spark, data_date):
    # type: (SparkSession,datetime.date) -> None
    if data_date is None:
        data_date = job_date
    date_vals = get_date_var(data_date)
    for dd in sorted(date_vals.keys()):
        spark.conf.set(dd, date_vals[dd])
    time_dict = get_galaxy_times(data_date.strftime('%Y-%m-%d'))
    for dd in sorted(time_dict.keys()):
        spark.conf.set(dd, time_dict[dd])


def quiet_spark_logs(sc):
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.WARN)
    log4j.LogManager.getLogger("org").setLevel(log4j.Level.ERROR)
    log4j.LogManager.getLogger("akka").setLevel(log4j.Level.ERROR)


def base64_encode_str(s):
    # type: (str) -> str
    encoded = base64.b64encode(s.encode(UTF_8))
    return encoded.decode(UTF_8)


def base64_decode_str(s):
    # type: (str) -> str
    # binascii.Error: Incorrect padding
    # https://stackoverflow.com/questions/2941995/python-ignore-incorrect-padding-error-when-base64-decoding
    s += '=' * (-len(s) % 4)
    decoded = base64.b64decode(s.encode(UTF_8))
    return decoded.decode(UTF_8)


def df_to_dict(df):
    # type: (pd.DataFrame) -> list[dict]
    return df.to_dict(orient='records')


def df_to_json(df):
    # type: (pd.DataFrame) -> str
    return df.to_json(orient='records', date_format='%Y-%m-%d %H:%M:%S')


def df_to_csv(df, file_path, sep=str('\t'), index=False, header=False, quoting=csv.QUOTE_MINIMAL):
    return df.to_csv(file_path, sep=sep, index=index, header=header, quoting=quoting, date_format='%Y-%m-%d %H:%M:%S', float_format='%.6f')


def sendmail(subject, content, receivers):
    import logging
    import smtplib
    from email.mime.text import MIMEText
    from email.header import Header
    try:
        from_mail = 'abs_report@jd.com'
        msg = MIMEText(content, 'html', 'utf-8')
        msg['Subject'] = subject  # 邮件主题
        # msg['From'] = Header(from_mail, 'utf-8')  # 发送者
        msg['From'] = from_mail
        msg['To'] = ','.join(receivers)  # 接收者
        # smtp server
        server = smtplib.SMTP("smtp.jd.local", 25)
        server.sendmail(from_mail, receivers, msg.as_string())
        server.quit()
        logging.warning('sendmail success:%s', subject)
    except Exception as e:
        logging.error(e)
        import traceback
        logging.error('sendmail fali:%s\n%s', subject, traceback.format_exc())
