#!/usr/bin/env python
# -*- coding: utf-8 -*-
# abs_product_mock.py
#

from __future__ import unicode_literals
from core.py_compat import *
import core.galaxy_task_boot as boot

boot.init()

def main(args=None):
    """
    main
    """
    import daily_job as daily_job
    return daily_job.do_job(args)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
