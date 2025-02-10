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

    # 读取配置文件
    config = configparser.ConfigParser()

    # 初始化并获取交易所信息
    print("初始化交易所数据...")
    obj_mysql_operator.init_exchange()
    
    # 记录总开始时间
    time_total_start = time.time()
    
    # 获取期货数据
    # ******************************************
    print("\n开始处理期货数据...")
    time_future_start = time.time()
    
    # 获得期货交易所信息
    print("获取期货交易所信息...")
    df_exchange_info = obj_mysql_operator.get_exchange(instrument_category="FUTURE")
    
    # 获取所有连续合约列表
    print("获取所有连续合约列表...")
    list_all_futures = xtdata.get_stock_list_in_sector('连续合约')
    # 过滤出包含"00."的合约
    list_continuous_futures = [str_future for str_future in list_all_futures if "00." in str_future]
    
    # 更新并获取期货连续合约详细信息
    print("更新并获取期货连续合约详细信息...")
    df_future_detail = obj_qmt_operator.get_instrument_detail(df_exchange_info, list_continuous_futures, "FUTURE")
    
    # 初始化并保存日志
    print("初始化数据下载日志表...")
    df_save_log = obj_mysql_operator.init_save_log(df_future_detail)
    
    # 下载数据
    config.read('./config/app.ini')
    # dt_init_begin = config.get('download', 'init_begin')
    dt_init_begin = '20240701000001'
    dt_init_end = datetime.now().strftime('%Y%m%d%H%M%S')
    print("开始下载期货数据...")
    obj_qmt_operator.download_barData(df_save_log, str_instrument_category="FUTURE", dt_init_begin=dt_init_begin, dt_init_end=dt_init_end)
    
    # 保存数据
    print("开始保存期货数据...")
    obj_qmt_operator.save_barData(str_instrument_category="FUTURE")
    
    # 计算期货数据处理时间
    time_future_elapsed = time.time() - time_future_start
    print(f"\n期货数据处理完成，耗时: {time_future_elapsed:.2f}秒")
    
    # 获取股票数据
    # 股票每次重新保存数据，以防止有复权和等比前复权
    # ******************************************
    # print("\n开始处理股票数据...")
    # time_stock_start = time.time()
    
    # # 获得股票交易所信息
    # print("获取股票交易所信息...")
    # df_exchange_info = obj_mysql_operator.get_exchange(instrument_category="STOCK")

    # # 获取各个股票列表
    # print("获取所有股票列表...")
    # list_all_stocks = []
    # for idx, row in df_exchange_info.iterrows():
    #     list_all_stocks = list_all_stocks + xtdata.get_stock_list_in_sector(row['ExchangeCName'])

    # # 更新并获取股票合约详细信息
    # print("更新并获取股票合约详细信息...")
    # df_stock_detail = obj_qmt_operator.get_instrument_detail(df_exchange_info, list_all_stocks, "STOCK")

    # # 初始化并保存日志
    # print("初始化股票数据下载日志表...")
    # df_save_log = obj_mysql_operator.init_save_log(df_stock_detail, str_instrument_category="STOCK")

    # # 下载数据
    # config.read('./config/app.ini')
    # dt_init_begin = config.get('download', 'init_begin')
    # dt_init_end = datetime.now().strftime('%Y%m%d%H%M%S')
    # print("开始下载股票数据...")
    # obj_qmt_operator.download_barData(df_save_log, str_instrument_category="STOCK", dt_init_begin=dt_init_begin, dt_init_end=dt_init_end)
    
    # # 保存数据
    # print("开始保存期货数据...")
    # obj_qmt_operator.save_barData(str_instrument_category="FUTSTOCKURE")
    
    # # 计算股票数据处理时间
    # time_stock_elapsed = time.time() - time_stock_start
    # print(f"\n股票数据处理完成，耗时: {time_stock_elapsed:.2f}秒")
    
    # 计算总耗时
    time_total_elapsed = time.time() - time_total_start
    print(f"\n所有数据处理完成，总耗时: {time_total_elapsed:.2f}秒")
    print(f"其中：")
    print(f"期货数据处理耗时: {time_future_elapsed:.2f}秒")
    # print(f"股票数据处理耗时: {time_stock_elapsed:.2f}秒")

    # 断开数据库连接
    obj_mysql_connect.disconnect()
       

    

# 