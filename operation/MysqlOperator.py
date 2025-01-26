import pandas as pd
from connect.MysqlConnect import MysqlConnect

class MysqlOperator:
    def __init__(self, mysql_connect: MysqlConnect):
        """初始化MySQL操作器"""
        self.mysql_connect = mysql_connect
        
    def get_exchange(self, instrument_category: str = "FUTURE") -> pd.DataFrame:
        """
        获取交易所信息
        
        Args:
            instrument_category: 交易品种类别，默认为"FUTURE"
        """
        try:
            query = """
                SELECT 
                    InstrumentCategory,
                    XTExchangeID,
                    ExchangeID,
                    ExchangeCName
                FROM D_base_Exchange
                WHERE InstrumentCategory = %s
                """
            # 注意：params必须是tuple类型
            result = self.mysql_connect.query(query, (instrument_category,))
            return result
        except Exception as e:
            print(f"获取交易所信息失败: {str(e)}")
            return pd.DataFrame()
            
    def upsert_futures_detail(self, df: pd.DataFrame) -> bool:
        """
        将期货合约详情更新或插入到D_base_code表中
        
        Args:
            df: 包含期货合约详情的DataFrame，已经与交易所信息合并
        
        Returns:
            bool: 操作是否成功
        """
        try:
            # 对DataFrame中的每一行进行处理
            for _, row in df.iterrows():
                # 检查记录是否存在
                check_query = """
                    SELECT COUNT(*) as count 
                    FROM D_base_code 
                    WHERE InstrumentID = %s
                """
                result = self.mysql_connect.query(check_query, (row['InstrumentID'],))
                exists = result.iloc[0]['count'] > 0

                if exists:
                    # 更新现有记录
                    update_query = """
                        UPDATE D_base_code SET
                            InstrumentName = %s,
                            ExchangeID = %s,
                            ProductID = %s,
                            ProductName = %s,
                            DeliveryYear = %s,
                            DeliveryMonth = %s,
                            VolumeMultiple = %s,
                            PriceTick = %s,
                            InstrumentCategory = %s,
                            XTExchangeID = %s,
                            ExchangeCName = %s,
                            PreClose = %s,
                            SettlementPrice = %s,
                            UpStopPrice = %s,
                            DownStopPrice = %s,
                            LongMarginRatio = %s,
                            ShortMarginRatio = %s,
                            LastVolume = %s,
                            ChargeType = %s,
                            CreateDate = %s,
                            OpenDate = %s,
                            ExchangeCode = %s,
                            InstrumentLongID = %s,
                            ExchangeLongCode = %s,
                            InstrumentJQLongID = %s
                        WHERE InstrumentID = %s
                    """
                    params = (
                        row['InstrumentName'],
                        row['ExchangeID'],
                        row['ProductID'],
                        row['ProductName'],
                        row['DeliveryYear'],
                        row['DeliveryMonth'],
                        row['VolumeMultiple'],
                        row['PriceTick'],
                        row['InstrumentCategory'],
                        row['XTExchangeID'],
                        row['ExchangeCName'],
                        row['PreClose'],
                        row['SettlementPrice'],
                        row['UpStopPrice'],
                        row['DownStopPrice'],
                        row['LongMarginRatio'],
                        row['ShortMarginRatio'],
                        row['LastVolume'],
                        row['ChargeType'],
                        row['CreateDate'],
                        row['OpenDate'],
                        row['ExchangeCode'],
                        row['InstrumentLongID'],
                        row['ExchangeLongCode'],
                        row['InstrumentJQLongID'],
                        row['InstrumentID']
                    )
                    self.mysql_connect.execute(update_query, params)
                else:
                    # 插入新记录
                    insert_query = """
                        INSERT INTO D_base_code (
                            InstrumentID,
                            InstrumentName,
                            ExchangeID,
                            ProductID,
                            ProductName,
                            DeliveryYear,
                            DeliveryMonth,
                            VolumeMultiple,
                            PriceTick,
                            InstrumentCategory,
                            XTExchangeID,
                            ExchangeCName,
                            PreClose,
                            SettlementPrice,
                            UpStopPrice,
                            DownStopPrice,
                            LongMarginRatio,
                            ShortMarginRatio,
                            LastVolume,
                            ChargeType,
                            CreateDate,
                            OpenDate,
                            ExchangeCode,
                            InstrumentLongID,
                            ExchangeLongCode,
                            InstrumentJQLongID
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s
                        )
                    """
                    params = (
                        row['InstrumentID'],
                        row['InstrumentName'],
                        row['ExchangeID'],
                        row['ProductID'],
                        row['ProductName'],
                        row['DeliveryYear'],
                        row['DeliveryMonth'],
                        row['VolumeMultiple'],
                        row['PriceTick'],
                        row['InstrumentCategory'],
                        row['XTExchangeID'],
                        row['ExchangeCName'],
                        row['PreClose'],
                        row['SettlementPrice'],
                        row['UpStopPrice'],
                        row['DownStopPrice'],
                        row['LongMarginRatio'],
                        row['ShortMarginRatio'],
                        row['LastVolume'],
                        row['ChargeType'],
                        row['CreateDate'],
                        row['OpenDate'],
                        row['ExchangeCode'],
                        row['InstrumentLongID'],
                        row['ExchangeLongCode'],
                        row['InstrumentJQLongID']
                    )
                    self.mysql_connect.execute(insert_query, params)
            
            return True
        except Exception as e:
            print(f"更新/插入期货合约详情失败: {str(e)}")
            return False

    def init_save_log(self, df_future_detail: pd.DataFrame) -> pd.DataFrame:
        """
        根据期货合约详细信息创建并保存日志到数据库
        
        Args:
            df_future_detail: 期货合约详细信息DataFrame
            
        Returns:
            pd.DataFrame: 保存日志DataFrame，如果失败返回空DataFrame
        """
        try:
            # 初始化保存日志dataframe
            list_save_log = []
            for _, row in df_future_detail.iterrows():
                # 处理连续合约
                list_save_log.append({
                    'InstrumentLongID': row['InstrumentLongID'],
                    'InstrumentID': row['InstrumentLongID'].split('.')[0],
                    'InstrumentCategory': row['InstrumentCategory'],
                    'XTExchangeID': row['XTExchangeID'],
                    'ExchangeID': row['ExchangeID'],
                    'ExchangeCName': row['ExchangeCName'],
                    'instrument_CName': row['ProductName']
                })
                
                # # 处理加权连续合约
                # list_save_log.append({
                #     'InstrumentLongID': row['InstrumentJQLongID'],
                #     'InstrumentID': row['InstrumentJQLongID'].split('.')[0],
                #     'InstrumentCategory': row['InstrumentCategory'],
                #     'XTExchangeID': row['XTExchangeID'],
                #     'ExchangeID': row['ExchangeID'],
                #     'ExchangeCName': row['ExchangeCName'],
                #     'instrument_CName': row['ProductName']
                # })
            
            # 创建DataFrame
            df_save_log = pd.DataFrame(list_save_log)
            if len(df_save_log) == 0:
                print("创建保存日志DataFrame失败: 没有数据")
                return pd.DataFrame()
                
            print(f"创建保存日志成功，共 {len(df_save_log)} 条记录")
            
            # 保存到数据库
            print("\n开始保存到数据库...")
            success_count = 0
            skip_count = 0
            
            # 对DataFrame中的每一行进行处理
            for _, row in df_save_log.iterrows():
                # 检查记录是否存在
                check_query = """
                    SELECT COUNT(*) as count 
                    FROM log_save 
                    WHERE InstrumentLongID = %s
                """
                result = self.mysql_connect.query(check_query, (row['InstrumentLongID'],))
                exists = result.iloc[0]['count'] > 0
                
                # 如果记录不存在，则插入
                if not exists:
                    insert_query = """
                        INSERT INTO log_save (
                            InstrumentLongID,
                            InstrumentID,
                            InstrumentCategory,
                            XTExchangeID,
                            ExchangeID,
                            ExchangeCName,
                            instrument_CName
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s
                        )
                    """
                    params = (
                        row['InstrumentLongID'],
                        row['InstrumentID'],
                        row['InstrumentCategory'],
                        row['XTExchangeID'],
                        row['ExchangeID'],
                        row['ExchangeCName'],
                        row['instrument_CName']
                    )
                    if self.mysql_connect.execute(insert_query, params):
                        success_count += 1
                else:
                    skip_count += 1
            
            print(f"保存完成: {success_count} 条新增, {skip_count} 条已存在")
            return df_save_log
            
        except Exception as e:
            print(f"初始化保存日志失败: {str(e)}")
            return pd.DataFrame()

    def get_save_date(self, instrument_long_id: str) -> tuple:
        """
        从log_save表中获取指定合约的开始和结束日期
        
        Args:
            instrument_long_id: 合约长代码（主键）
            
        Returns:
            tuple: (begin_date, end_date)，如果查询失败返回(None, None)
        """
        try:
            query = """
                SELECT begin_date, end_date
                FROM log_save
                WHERE InstrumentLongID = %s
            """
            result = self.mysql_connect.query(query, (instrument_long_id,))
            
            if len(result) > 0:
                return result.iloc[0]['begin_date'], result.iloc[0]['end_date']
            else:
                print(f"未找到合约记录: {instrument_long_id}")
                return None, None
                
        except Exception as e:
            print(f"获取合约日期失败: {str(e)}")
            return None, None

    def update_save_date(self, instrument_long_id: str, begin_date: str, end_date: str) -> bool:
        """
        更新log_save表中指定合约的开始和结束日期
        
        Args:
            instrument_long_id: 合约长代码（主键）
            begin_date: 开始日期
            end_date: 结束日期
            
        Returns:
            bool: 更新是否成功
        """
        try:
            # 更新日期
            update_query = """
                UPDATE log_save
                SET begin_date = %s,
                    end_date = %s
                WHERE InstrumentLongID = %s
            """
            params = (begin_date, end_date, instrument_long_id)
            
            if self.mysql_connect.execute(update_query, params):
                print(f"更新日期成功: {instrument_long_id}")
                print(f"数据范围: {begin_date} - {end_date}")
                return True
            else:
                print(f"更新日期失败: {instrument_long_id}")
                return False
                
        except Exception as e:
            print(f"更新合约日期失败: {str(e)}")
            return False

# 示例使用
# if __name__ == "__main__":
#     # 创建数据库连接
#     mysql_connect = MysqlConnect()
#     if not mysql_connect.connect():
#         exit()
        
#     # 创建操作器
#     operator = MysqlOperator(mysql_connect)
    
#     # 获取交易所信息
#     exchanges = operator.get_exchange()
#     print("交易所信息:")
#     print(exchanges)
    
#     # 断开连接
#     mysql_connect.disconnect()

