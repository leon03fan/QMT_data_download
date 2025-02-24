import sys
import os
from datetime import datetime
from contextlib import contextmanager

class logPrintRedirector:
    """
    增强版打印重定向器
    可以同时输出到终端和日志文件，日志文件中的内容会自动添加时间戳
    """
    def __init__(self):
        """初始化重定向器"""
        self.terminal = sys.stdout
        self.log_folder = 'logs'
        
        # 确保log文件夹存在
        if not os.path.exists(self.log_folder):
            os.makedirs(self.log_folder)
    
    def get_log_filename(self):
        """获取当前日期的日志文件名"""
        return os.path.join(self.log_folder, f"{datetime.now().strftime('%Y%m%d')}.log")
    
    def write(self, message):
        """
        写入消息到终端和文件
        终端显示原始消息
        文件中添加时间戳
        """
        # 输出到终端
        self.terminal.write(message)
        
        # 写入到文件（添加时间戳）
        if message.strip():  # 只处理非空消息
            # 处理可能的多行消息
            lines = message.splitlines()
            for line in lines:
                if line.strip():  # 跳过空行
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    formatted_message = f"【{timestamp}】 - {line}\n"
                    with open(self.get_log_filename(), 'a', encoding='utf-8') as log_file:
                        log_file.write(formatted_message)
    
    def flush(self):
        """刷新缓冲区"""
        self.terminal.flush()
    
    @contextmanager
    def redirect_to_file(self):
        """
        上下文管理器，用于临时重定向输出到文件
        """
        old_stdout = sys.stdout
        sys.stdout = self
        try:
            yield
        finally:
            sys.stdout = old_stdout

    def _format_message(self, message):
        """添加时间戳"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        return f"[{timestamp}] {message}"