import mysql.connector
import pandas as pd
import configparser
import os

class MysqlConnect:
    def __init__(self):
        """初始化MySQL操作器，从配置文件读取连接信息"""
        self.conn = None
        self._load_config()
        self.cursor = None
        
    def _load_config(self):
        """加载配置文件"""
        try:
            config = configparser.ConfigParser()
            config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'Mysql.ini')
            config.read(config_path)
            
            # 读取MySQL配置
            self.host = config['mysql']['host']
            self.user = config['mysql']['user']
            self.password = config['mysql']['password']
            self.database = config['mysql']['database']
        except Exception as e:
            print(f"读取配置文件失败: {str(e)}")
            # 使用默认值
            self.host = "localhost"
            self.user = "root"
            self.password = "111111"
            self.database = "hd_database"
            
    def connect(self):
        """建立数据库连接"""
        try:
            self.conn = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            print(f"数据库连接成功。")
            return True
        except Exception as e:
            print(f"数据库连接失败: {str(e)}")
            return False
            
    def disconnect(self):
        """关闭数据库连接"""
        if self.conn and self.conn.is_connected():
            self.conn.close()
            self.conn = None
            
    def execute(self, query, values=None):
        """执行SQL语句"""
        if not self.conn:
            if not self.connect():
                return False
                
        try:
            cursor = self.conn.cursor()
            if values:
                cursor.execute(query, values)
            else:
                cursor.execute(query)
            self.conn.commit()
            cursor.close()
            return True
        except Exception as e:
            print(f"执行SQL出错: {str(e)}")
            return False
            
    def query(self, query, params=None) -> pd.DataFrame:
        """
        执行查询语句并返回DataFrame
        
        Args:
            query: SQL查询语句
            params: 查询参数，可以是tuple或list
        """
        try:
            cursor = self.conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
                
            # 获取列名和数据
            columns = [desc[0] for desc in cursor.description]
            result = pd.DataFrame(cursor.fetchall(), columns=columns)
            
            cursor.close()
            return result
        except Exception as e:
            print(f"查询数据出错: {str(e)}")
            return pd.DataFrame()
            
    def __enter__(self):
        """上下文管理器入口"""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.disconnect()

# 示例使用
# if __name__ == "__main__":
#     with MysqlOperator() as db:
#         data = db.query("SELECT * FROM D_base_Exchange")
#         if data is not None:
#             print(data)