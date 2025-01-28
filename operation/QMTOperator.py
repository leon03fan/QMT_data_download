from datetime import datetime
import time
import pandas as pd
from xtquant import xtdata
from connect.QMTConnect import QMTConnect
from connect.MysqlConnect import MysqlConnect
from operation.MysqlOperator import MysqlOperator
from utility import utility
import configparser
import os
from pickle import dump

class QMTOperator:
    def __init__(self, qmt_connect: QMTConnect, mysql_connect: MysqlConnect):
        """
        初始化QMT操作器
        
        Args:
            qmt_connect: QMT连接实例
            mysql_connect: MySQL连接实例
        """
        self.qmt_connect = qmt_connect
        self.mysql_operator = MysqlOperator(mysql_connect)
        
    def get_instrument_detail(self, df_exchange_info: pd.DataFrame, list_instrumentID: list, str_InstrumentCategory: str) -> bool:
        """
        获取合约详细信息并保存到数据库
        
        Args:
            df_exchange_info: 交易所信息DataFrame
            list_instrumentID: 对于期货来说是连续合约代码列表 对于股票来说是股票代码
            str_InstrumentCategory: 品种类型，"FUTURE"为期货，"STOCK"为股票
            
        Returns:
            bool: 操作是否成功
        """
        try:
            # 准备存储详细信息的列表
            list_futures_detail = []
            
            # 遍历连续合约获取详细信息
            print("获取合约详细信息...")
            for str_future in list_instrumentID:
                dict_detail = xtdata.get_instrument_detail(str_future, True)
                if dict_detail:
                    list_futures_detail.append({
                        'InstrumentID': dict_detail.get('InstrumentID', ''),  # 合约代码
                        'ExchangeID': dict_detail.get('ExchangeID', ''),  # 市场简称
                        'InstrumentName': dict_detail.get('InstrumentName', ''),  # 合约名称
                        'ExchangeCode': dict_detail.get('ExchangeCode', ''),  # 交易所产品代码
                        'Abbreviation': dict_detail.get('Abbreviation', ''),  # 合约名称的拼音简写
                        'ProductID': dict_detail.get('ProductID', ''),  # 合约的品种ID(期货)
                        'ProductName': dict_detail.get('ProductName', ''),  # 合约的品种名称(期货)
                        'UnderlyingCode': dict_detail.get('UnderlyingCode', ''),  # 标的合约
                        'CreateDate': dict_detail.get('CreateDate', ''),  # 上市日期(期货)
                        'OpenDate': dict_detail.get('OpenDate', ''),  # IPO日期(股票)
                        'ExpireDate': dict_detail.get('ExpireDate', ''),  # 退市日或者到期日
                        'PreClose': dict_detail.get('PreClose', 0),  # 前收盘价格
                        'SettlementPrice': dict_detail.get('SettlementPrice', 0),  # 前结算价格
                        'UpStopPrice': dict_detail.get('UpStopPrice', 0),  # 当日涨停价
                        'DownStopPrice': dict_detail.get('DownStopPrice', 0),  # 当日跌停价
                        'FloatVolume': dict_detail.get('FloatVolume', 0),  # 流通股本
                        'TotalVolume': dict_detail.get('TotalVolume', 0),  # 总股本
                        'LongMarginRatio': dict_detail.get('LongMarginRatio', 0),  # 多头保证金率
                        'ShortMarginRatio': dict_detail.get('ShortMarginRatio', 0),  # 空头保证金率
                        'PriceTick': dict_detail.get('PriceTick', ''),  # 最小变价单位
                        'VolumeMultiple': dict_detail.get('VolumeMultiple', 0),  # 合约乘数
                        'LastVolume': dict_detail.get('LastVolume', 0),  # 昨日持仓量
                        'DeliveryYear': dict_detail.get('DeliveryYear', ''),  # 交割年份
                        'DeliveryMonth': dict_detail.get('DeliveryMonth', ''),  # 交割月
                        'ChargeType': dict_detail.get('ChargeType', 0),  # 期货和期权手续费方式
                    })
            
            # 创建DataFrame
            df_futures_detail = pd.DataFrame(list_futures_detail)
            
            # 重命名df_exchange_info的列名，以匹配合并需求
            df_exchange_info = df_exchange_info.rename(columns={
                'instrumentCategory': 'InstrumentCategory',
                'xtexchangeId': 'XTExchangeID',
                'exchangeId': 'ExchangeID',
                'exchangeCName': 'ExchangeCName'
            })
            
            # 使用merge合并两个DataFrame
            df_futures_merged = pd.merge(
                df_futures_detail,
                df_exchange_info,
                on='ExchangeID',
                how='left'
            )
            if str_InstrumentCategory == "FUTURE":
                df_futures_merged["InstrumentLongID"] = df_futures_merged["InstrumentID"] + "." + df_futures_merged["XTExchangeID"] # 连续合约长代码
                df_futures_merged["ExchangeLongCode"] = df_futures_merged["ExchangeCode"] + "." + df_futures_merged["XTExchangeID"] # 交易所产品长代码--主力合约
                df_futures_merged["InstrumentJQLongID"] = df_futures_merged["InstrumentID"].str[:-2] + "JQ00." + df_futures_merged["XTExchangeID"] # 加权连续合约长代码
            elif str_InstrumentCategory == "STOCK":
                df_futures_merged["InstrumentLongID"] = df_futures_merged["InstrumentID"] + "." + df_futures_merged["XTExchangeID"] # 连续合约长代码
                df_futures_merged["ExchangeLongCode"] = "" # 交易所产品长代码--主力合约
                df_futures_merged["InstrumentJQLongID"] = "" # 加权连续合约长代码

            
            # 将合并后的数据保存到数据库
            print("开始保存合约详细信息数据到数据库...")
            if self.mysql_operator.upsert_futures_detail(df_futures_merged):
                print("合约详细信息数据保存成功！")
                return df_futures_merged
            else:
                print("合约详细信息数据保存失败！")
                return None
            
        except Exception as e:
            print(f"获取合约详细信息失败: {str(e)}")
            return None

    def download_and_save_barData(self, df_save_log: pd.DataFrame, str_instrument_category: str = "FUTURE"):
        """
        下载并保存K线数据
        
        Args:
            df_save_log: 保存日志DataFrame
            str_instrument_category: 品种类型，"FUTURE"为期货，"STOCK"为股票
        """
        # 读取配置文件
        config = configparser.ConfigParser()
        config.read('./config/app.ini')
        if str_instrument_category == "FUTURE":
            str_data_save_path = config.get('path', 'future_data_path')
        elif str_instrument_category == "STOCK":
            str_data_save_path = config.get('path', 'stock_data_path')
        
        # 获取初始下载时间
        dt_init_begin = config.get('download', 'init_begin')
        dt_init_end = datetime.now().strftime('%Y%m%d%H%M%S')
        # dt_init_end = "20240301000001"
        
        # 记录总开始时间
        if str_instrument_category == "FUTURE":
            list_interpreters = ["tick","1m","5m","15m","1h","1d"]
            # list_interpreters = ["5m","15m","1h","1d"] # 测试用
        elif str_instrument_category == "STOCK":
            list_interpreters = ["1m","5m","15m","1h","1d"] # 为了节约时间 股票数据不下载tick数据
        time_total_start = time.time()
        cnt = 0
        for _, row in df_save_log.iterrows():
            # 测试开关
            # if cnt > 1:
            #     break
            # if row['InstrumentLongID'] != "ag00.SF":
            #     continue

            # 检查是否为指定品种
            if row['InstrumentCategory'] != str_instrument_category:
                continue
                
            # 记录当前产品开始时间 用于计算当前产品耗时
            time_product_start = time.time()
            
            # 获取当前产品开始结束时间
            dt_download_begin, dt_download_end = self.mysql_operator.get_save_date(row['InstrumentLongID'])
            if dt_download_begin is None:
                dt_download_begin = dt_init_begin
                dt_download_end = dt_init_end
            else:
                dt_download_begin = dt_download_end
                dt_download_end = dt_init_end
                
            # 下载数据
            for i in list_interpreters:
                # 记录当前周期开始时间
                time_period_start = time.time()
                try:
                    # 下载数据
                    xtdata.download_history_data(row['InstrumentLongID'], i, start_time=dt_download_begin, end_time=dt_download_end)
                    # 计算当前周期耗时
                    time_period_elapsed = time.time() - time_period_start
                    print(f"产品：{row['ExchangeCName']}-{row['instrument_CName']}-{row['InstrumentLongID']} {i}周期下载完成，耗时: {time_period_elapsed:.2f}秒")
                except Exception as e:
                    print(f"产品：{row['ExchangeCName']}-{row['instrument_CName']}-{row['InstrumentLongID']} {i}周期下载出错: {str(e)}")
                    continue
                
            # 保存数据到本地barData目录下 样式为pkl文件
            # 期货数据没有前复权，所以只要直接叠加就可以。
            if str_instrument_category == "FUTURE":
                list_dividend_type = ["none"]
                for i in list_interpreters:
                    for dividend_type in list_dividend_type:
                            dict_result = xtdata.get_market_data_ex([], [row['InstrumentLongID']], period=i, dividend_type=dividend_type,
                                start_time=dt_download_begin, end_time=dt_download_end,
                                count=-1, fill_data=False)
                            df_temp = dict_result[row['InstrumentLongID']]
                            df_temp.index = utility.batch_timestamp_to_string_17(df_temp["time"])
                            utility.append_to_pkl(df_temp, f"{str_data_save_path}/{row['ExchangeCName']}-{dividend_type}-{row['instrument_CName']}-{row['InstrumentLongID']}-{i}.pkl")
            # 股票数据需要前复权，所以不能叠加，只能全部数据保存
            elif str_instrument_category == "STOCK":
                list_dividend_type = ["none","front_ratio"] # 前复权和等比前复权 这里为了节省空间 只保存等比前复权。 "front" 是前复权，"front_ratio" 是等比前复权。
                for i in list_interpreters:
                    for dividend_type in list_dividend_type:
                            dict_result = xtdata.get_market_data_ex([], [row['InstrumentLongID']], period=i, dividend_type=dividend_type,
                                start_time=dt_init_begin, end_time=dt_download_end,
                                count=-1, fill_data=False)
                            df_temp = dict_result[row['InstrumentLongID']]
                            df_temp.index = utility.batch_timestamp_to_string_17(df_temp["time"])
                            df_temp.to_pickle(f"{str_data_save_path}/{row['ExchangeCName']}-{dividend_type}-{row['instrument_CName']}-{row['InstrumentLongID']}-{i}.pkl")
                
            # 保存记录到数据库
            self.mysql_operator.update_save_date(row['InstrumentLongID'], dt_init_begin, dt_download_end)
            
            # 计算并显示当前产品总耗时
            time_product_elapsed = time.time() - time_product_start
            cnt += 1
            print(f"已下载 {cnt} 个产品，本产品总耗时: {time_product_elapsed:.2f}秒")
            
        # 计算并显示总耗时
        time_total_elapsed = time.time() - time_total_start
        print(f"\n所有数据下载完成，总耗时: {time_total_elapsed:.2f}秒")