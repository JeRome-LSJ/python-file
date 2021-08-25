#!/usr/bin/env python
# -*- coding: utf-8 -*-
# hive_runer_task.py
#

from __future__ import unicode_literals

import logging

from core import hive_executor

_logger = logging.getLogger('abs_mock')
_logger.setLevel(logging.INFO)


def do_hive_stat():
    sql_map = {
        "sql_01": """
            alter table dmz_sfp.dmzsfp_sfp_bt_abs_am_abs_asst_inc_i_d drop partition (dt<'{TX_PRE_7_DATE}');
        """,

        "sql_02": """
            alter table dmz_sfp.dmzsfp_sfp_bt_abs_am_abs_asst_dtl_merge_s_d drop partition (dt<'{TX_PRE_14_DATE}');
        """,

        "sql_03": """
            alter table dmz_sfp.dmzsfp_sfp_bt_abs_am_abs_asst_merg_tmp_00_i_d drop partition (dt<'{TX_PRE_7_DATE}');
        """,

        # "sql_04": """
        #     alter table dmz_sfp.dmzsfp_sfp_jt_abs_am_abs_investor_installment_s_d drop partition (dt<'{TX_PRE_7_DATE}');
        # """,

        "sql_05": """
            alter table dmz_sfp.dmzsfp_sfp_jt_abs_am_abs_asst_dtl_merge_tmp_00_i_d drop partition (dt<'{TX_PRE_7_DATE}');
        """,

        "sql_06": """
            alter table dmz_sfp.dmzsfp_sfp_jt_abs_am_abs_asst_dtl_merge_s_d drop partition (dt<'{TX_PRE_14_DATE}');
        """,
    }
    hive_executor.run_hql_map(sql_map)
    # alter table dmz_sfp.dmzsfp_sfp_bt_abs_am_abs_asst_dtl_o_s_d drop partition (dt<'{TX_PRE_2_DATE}');

    # 
    # 
    # 
    # 
    # 
    # 
    # alter table dmz_sfp.dmzsfp_sfp_bt_abs_am_abs_asst_dtl_tmp_01_i_d drop partition (dt<'{TX_PRE_7_DATE}');
    # alter table dmz_sfp.dmzsfp_sfp_jt_abs_am_abs_sync_asst_dtl_merge_tmp_00_i_d drop partition (dt<'{TX_PRE_7_DATE}');
    # alter table dmz_sfp.dmzsfp_sfp_jt_abs_am_abs_asst_dtl_tmp_00_i_d drop partition (dt<'{TX_PRE_7_DATE}');
    # alter table dmz_sfp.dmzsfp_sfp_jt_abs_am_abs_asst_dtl_o_s_d drop partition (dt<'{TX_PRE_2_DATE}');
    # alter table dmz_sfp.dmzsfp_sfp_jt_abs_am_abs_loan_event_i_d drop partition (dt<'{TX_PRE_14_DATE}');
    # alter table dmz_sfp.dmzsfp_sfp_bt_abs_am_abs_asst_dtl_tmp_00_i_d drop partition (dt<'{TX_PRE_7_DATE}');
    # alter table dmz_sfp.dmzsfp_sfp_bt_abs_am_abs_asst_dtl_sync_tmp_00_i_d drop partition (dt<'{TX_PRE_7_DATE}');
    # alter table dmz_sfp.dmzsfp_sfp_jt_abs_am_abs_investor_asst_dtl_tmp_00_i_d drop partition (dt<'{TX_PRE_7_DATE}');
    # alter table dmz_sfp.dmzsfp_sfp_jt_abs_am_abs_asst_inc_i_d drop partition (dt<'{TX_PRE_7_DATE}');
    # alter table dmz_sfp.dmzsfp_sfp_common_am_abs_spec_plan_acct_chk_tmp_s_d drop partition (dt<'{TX_PRE_31_DATE}');
    # alter table dmz_sfp.dmzsfp_sfp_bt_abs_am_abs_user_specplan_amt_lmt_i_d drop partition (dt<'{TX_PRE_7_DATE}');
    # alter table dmz_sfp.dmzsfp_sfp_jt_abs_am_abs_investor_asst_dtl_tmp_02_i_d drop partition (dt<'{TX_PRE_7_DATE}');
    # alter table dmz_sfp.dmzsfp_sfp_jt_abs_am_abs_asst_filter_i_d drop partition (dt<'{TX_PRE_7_DATE}');
    # alter table dmz_sfp.dmzsfp_sfp_bt_abs_am_abs_asst_filter_i_d drop partition (dt<'{TX_PRE_7_DATE}');

def start_hive_task():
    do_hive_stat()