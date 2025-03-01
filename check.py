import numpy as np
import pandas as pd
import pickle
import os
from datetime import datetime

def check_future_data():
    """
    读取并检查期货数据文件
    """
    # 文件路径
    file_path = os.path.join('barData', 'FUTURE', '大商所-豆一-a00.DF-none-15m.pkl')
    
    try:
        # 读取pkl文件
        print(f"\n开始读取文件: {file_path}")
        df = pd.read_pickle(file_path)
        
        # 显示基本信息
        print("\n数据基本信息:")
        print(f"数据行数: {len(df)}")
        print(f"数据列: {df.columns.tolist()}")
        
        # 显示时间范围
        print("\n数据时间范围:")
        print(f"开始时间: {df.index[0]}")
        print(f"结束时间: {df.index[-1]}")
        
        # 显示数据样例
        print("\n数据样例（前5行）:")
        print(df.head())
        
        # 显示数据统计信息
        print("\n数据统计信息:")
        print(df.describe())
        
        # 创建日期统计DataFrame
        print("\n开始统计每日bar数量...")
        
        # 将索引转换为日期时间格式（如果不是的话）
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
            
        # 创建日期统计
        df_stats = pd.DataFrame({
            '日期': df.index.date,
            '周几': df.index.dayofweek + 1,  # 将0-6转换为1-7
            'bar数量': 1
        })
        
        # 按日期分组统计
        df_stats = df_stats.groupby(['日期', '周几']).count().reset_index()
        
        # 按日期排序
        df_stats = df_stats.sort_values('日期', ascending=True)
        
        # 显示最后100行记录
        print("\n最后100个交易日的bar数量统计:")
        print(df_stats.tail(100))
        
        return df, df_stats
        
    except FileNotFoundError:
        print(f"错误：文件不存在 - {file_path}")
        return None, None
    except Exception as e:
        print(f"错误：读取文件时发生错误 - {str(e)}")
        return None, None

if __name__ == "__main__":
    # 创建日志重定向器
    from logPrintRedirector import logPrintRedirector
    redirector = logPrintRedirector()
    
    # 使用重定向器记录输出
    with redirector.redirect_to_file():
        print("开始检查期货数据文件...")
        df, df_stats = check_future_data()
        # if df is not None:
        #     # 保存统计结果到CSV文件
        #     stats_file = 'bar_statistics.csv'
        #     df_stats.to_csv(stats_file, index=False, encoding='utf-8-sig')
        #     print(f"\n统计结果已保存到文件: {stats_file}")
        #     print("数据检查完成！")

