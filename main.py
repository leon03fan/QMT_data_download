import time
import numpy as np
import pandas as pd
from datetime import datetime
from joblib import dump, load
import configparser

from xtquant import xtdata
from xtquant import xtdatacenter as xtdc

from connect.QMTConnect import QMTConnect
from connect.MysqlConnect import MysqlConnect
from operation.MysqlOperator import MysqlOperator
from operation.QMTOperator import QMTOperator
from utility import utility

import warnings
warnings.filterwarnings("ignore")


if __name__ == "__main__":
    # 读取配置文件
    config = configparser.ConfigParser()
    config.read('config/app.ini')
    str_data_save_path = config.get('path', 'data_save_path')
    
    # 连接QMT
    obj_qmt = QMTConnect()
    if not obj_qmt.connect():
        exit()
        
    # 连接数据库
    obj_mysql_connect = MysqlConnect()
    if not obj_mysql_connect.connect():
        exit()
        
    # 创建数据库操作器
    obj_mysql_operator = MysqlOperator(obj_mysql_connect)
    # 创建QMT操作器
    obj_qmt_operator = QMTOperator(obj_qmt, obj_mysql_connect)

    # 初始化并获取交易所信息
    print("初始化交易所数据...")
    obj_mysql_operator.init_exchange()
    print("\n获取交易所信息...")
    df_exchange_info = obj_mysql_operator.get_exchange(instrument_category="FUTURE")
    
    # 获取期货数据
    # ******************************************
    # 获取所有连续合约列表
    list_all_futures = xtdata.get_stock_list_in_sector('连续合约')
    
    # 过滤出包含"00."的合约
    list_continuous_futures = [str_future for str_future in list_all_futures if "00." in str_future]
    
    # 更新并获取期货连续合约详细信息
    print("更新并获取期货连续合约详细信息...")
    df_future_detail = obj_qmt_operator.get_futures_detail(df_exchange_info, list_continuous_futures)
    
    # 初始化并保存日志
    print("初始化数据下载日志表...")
    df_save_log = obj_mysql_operator.init_save_log(df_future_detail)
    
    # 下载数据
    print("开始下载数据...")
    obj_qmt_operator.download_and_save_barData(df_save_log)
    
    # 断开数据库连接
    obj_mysql_connect.disconnect()
       

    

# 