#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# BootScript.py

import os
import sys
import time
import glob
import logging
import subprocess

boot_dir = os.path.split(os.path.realpath(__file__))[0]
logging.warning('boot_dir:%s', boot_dir)


def search_file(path, reg='*'):
    # type: (str,str) -> list[str]
    return glob.glob(os.path.abspath(os.path.join(path, reg)))


def init(_path=None):
    if _path is None:
        _path = boot_dir + '/pkgs'
    _path = os.path.abspath(_path)
    # Add Module
    _whls = search_file(_path, '*.whl')
    logging.warning('Search whls:%s', _whls)
    if _whls:
        sys.path[:0] = _whls
    else:
        logging.warning('Not Found whl in:' + _path)
    _egg = search_file(_path, '*.egg')
    logging.warning('Search eggs:%s', _egg)
    if _egg:
        sys.path.extend(_egg)
    else:
        logging.warning('Not Found egg in:' + _path)

    sys.path.insert(0, _path)
    sys.path.insert(0, '/tmp/galaxy_boot')

    logging.warning('workon:%s', get_ip_addr())
    run_sh()


def get_ip_addr():
    import socket
    try:
        hostname = socket.gethostname()
        ipaddrs = socket.gethostbyname(hostname)
        return hostname, ipaddrs
    except Exception as e:
        logging.warning(e)
        return 'localhost', '127.0.0.1'


def run_sh():
    proc = subprocess.Popen(os.path.abspath(boot_dir + '/galaxy_boot.sh'), shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    logging.warning('galaxy_boot.sh:%d', proc.pid)
    time_start = time.time()
    while proc.poll() is None:
        out = proc.stdout.readline()
        if out:
            logging.warning('galaxy_boot.sh:%s', out.rstrip('\r\n'))
    logging.warning('galaxy_boot.sh:[%s], time cost %d sec.', proc.returncode, (time.time() - time_start))
    # outs, errs = proc.communicate()
    # logging.warning('galaxy_boot.sh:[%s], outs:%s', proc.returncode, outs)
    # logging.warning('galaxy_boot.sh:errs:%s', errs)
    return proc.returncode


# test
if __name__ == "__main__":
    print(__file__)
    init('../dist')
