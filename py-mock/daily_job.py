#!/usr/bin/env python
# -*- coding: utf-8 -*-
# daily_job.py
#

from __future__ import unicode_literals
import hive_runer_task

import time
import logging
import traceback

from core import spark5k

_logger = logging.getLogger('abs_mock')
_logger.setLevel(logging.INFO)


def do_job(args=None):
    _logger.info('args:%s', args)
    time_start = time.time()

    spark5k.init_args(args)
    # 初始化 spark
    spark_session = spark5k.init_spark_session('daily_job')

    try:
        # 执行资产还款退款统计及更新
        hive_runer_task.start_hive_task()

    except Exception as e:
        _logger.error(e)
        _logger.error('作业运行失败: %s', traceback.format_exc())
        return 1
    finally:
        _logger.warning('time cost %d sec.', (time.time() - time_start))
        spark_session.stop()
    return 0
