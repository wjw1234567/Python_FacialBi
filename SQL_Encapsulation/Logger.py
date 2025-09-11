import os
from datetime import datetime

class Logger:
    def __init__(self, log_dir="./logs", prefix="trackheatmap"):
        self.log_dir = log_dir
        self.prefix = prefix
        self.current_month = None
        self.log_file = None
        os.makedirs(log_dir, exist_ok=True)

    def _get_log_path(self):
        now = datetime.now()
        month_str = now.strftime("%Y-%m")
        if month_str != self.current_month:
            self.current_month = month_str
            if self.log_file:
                self.log_file.close()
            log_path = os.path.join(self.log_dir, f"{self.prefix}_{month_str}.log")
            self.log_file = open(log_path, "a", encoding="utf-8")
        return self.log_file

    def log(self, message, level="INFO"):
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"{now} [{level}] {message}\n"
        f = self._get_log_path()
        f.write(line)
        f.flush()   # 保证实时写入
        print(line, end="")  # 控制台同步输出

    def error(self, message):
        self.log(message, level="ERROR")
