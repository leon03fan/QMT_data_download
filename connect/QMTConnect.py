import configparser
import os
from xtquant import xtdatacenter as xtdc
from xtquant import xtdata

class QMTConnect:
    def __init__(self):
        """初始化QMT操作器"""
        self.connected = False
        self._load_config()
        
    def _load_config(self):
        """加载配置文件"""
        try:
            config = configparser.ConfigParser()
            config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'QMT.ini')
            config.read(config_path)
            
            # 读取QMT配置
            self.token = config['qmt']['token']
        except Exception as e:
            print(f"读取配置文件失败: {str(e)}")

            
    def connect(self):
        """连接迅投服务器"""
        xtdc.set_token(self.token)
        try:
            xtdc.init()
            self.connected = True
            print("连接QMT服务器成功。")
            return True
        except Exception as e:
            print(e)
            print(f"连接QMT服务器失败: {str(e)}")
            self.connected = False
            return False
            
    @property
    def is_connected(self):
        """检查连接状态"""
        return self.connected

# 示例使用
# if __name__ == "__main__":
#     qmt = QMTOperator()
#     qmt.connect()
    
