from joblib import dump, load
import numpy as np
import pandas as pd
import os
from datetime import datetime, timezone, timedelta
import pyarrow as pa
import pyarrow.parquet as pq

class utility:
    """数据工具类，用于处理数据保存等操作"""
    
    @staticmethod
    def append_to_pkl(df_new: pd.DataFrame, str_file_path: str) -> bool:
        """
        将新数据追加到已有的pkl文件中，如果文件不存在则创建新文件
        
        Args:
            df_new (pd.DataFrame): 需要追加的新数据，index为数字格式的字符串（8-19位）
            str_file_path (str): pkl文件路径
            
        Returns:
            bool: 操作是否成功
        """
        try:
            print(f"\n开始处理文件: {str_file_path}")
            
            # 检查DataFrame是否为空
            if df_new is None or df_new.empty:
                print("警告：新的数据为空，不需要进行pkl添加操作")
                return False
                
            # 确保索引是字符串类型
            df_new.index = df_new.index.astype(str)
            print(f"新数据范围: {min(df_new.index)} 到 {max(df_new.index)}")
            print(f"新数据行数: {len(df_new)}")
            
            # 确保目录存在
            os.makedirs(os.path.dirname(str_file_path), exist_ok=True)
            
            # 如果文件已存在，读取并追加
            if os.path.exists(str_file_path):
                print("文件已存在，读取现有数据...")
                df_existing = pd.read_pickle(str_file_path)
                # 确保现有数据的索引也是字符串类型
                df_existing.index = df_existing.index.astype(str)
                print(f"现有数据范围: {min(df_existing.index)} 到 {max(df_existing.index)}")
                print(f"现有数据行数: {len(df_existing)}")
                
                # 合并数据
                df_combined = pd.concat([df_existing, df_new], axis=0)
                print(f"合并后初始行数: {len(df_combined)}")
                
                # 按字符串索引排序
                df_combined = df_combined.sort_index()
                
                # 去重
                df_combined = df_combined[~df_combined.index.duplicated(keep='last')]
                
                print(f"合并后最终数据范围: {min(df_combined.index)} 到 {max(df_combined.index)}")
                print(f"合并后最终行数: {len(df_combined)}")
                
                # 验证数据是否正确合并
                if len(df_combined) < len(df_existing):
                    print("警告：合并后的数据行数小于现有数据行数！")
                    return False
                
                df_combined.to_pickle(str_file_path)
                print("数据已保存")
            else:
                print("文件不存在，创建新文件...")
                # 直接保存
                df_new = df_new.sort_index()
                df_new.to_pickle(str_file_path)
                print("数据已保存")
            
            return True
            
        except Exception as e:
            print(f"保存数据时发生错误: {str(e)}")
            print(f"错误详情: {e.__class__.__name__}")
            return False

    def batch_timestamp_to_string_17(timestamps):
        """
        批量处理时间戳列表 将毫秒级时间戳转换为17位字符串
        """
        def timestamp_to_string_17(timestamp_ms):
            """
            将毫秒级时间戳转换为17位字符串
            格式：YYYYMMDDHHMMSSMMM
            示例：20240131211318123
            """
            # 创建UTC时间
            utc_time = datetime.fromtimestamp(timestamp_ms/1000, timezone.utc)
            # 转换为东八区
            cn_timezone = timezone(timedelta(hours=8))
            cn_time = utc_time.astimezone(cn_timezone)
            # 格式化为17位字符串
            return cn_time.strftime('%Y%m%d%H%M%S') + f'{int(timestamp_ms % 1000):03d}'
        return [timestamp_to_string_17(ts) for ts in timestamps]

    def save_pyarrow(self, df, file_path):
        """
        创建新的parquet文件
        :param df: 要保存的DataFrame
        """
        try:
            # 将DataFrame转换为parquet并保存
            df.to_parquet(
                file_path,
                engine='pyarrow',
                compression='snappy',  # 使用snappy压缩
                index=False  # 不保存索引
            )
            print(f"成功保存数据，行数: {len(df)}")
            return True
        except Exception as e:
            print(f"保存数据失败: {str(e)}")
            return False