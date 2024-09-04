# -*- coding:UTF-8 -*-
"""
 @Author: CNN
 @FileName: 记录日志.py
 @DateTime: 2024/9/4 8:28
 @SoftWare: PyCharm
"""
import logging
import sys

# 1.配置日志记录器
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s -%(levelname)s -%(message)s',
    handlers=[
        logging.FileHandler('output2.log'),
        logging.StreamHandler(sys.stdout)
    ]
)


#
class StreamToLogger:

    def __init__(self, logger, log_level=logging.INFO):
        self.logger = logger
        self.log_level = log_level
        self.linebuf = ''

    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.log_level, line)

    def flush(self):
        pass


if __name__ == "__main__":
    sys.stdout = StreamToLogger(logging.getLogger('STDOUT'), logging.INFO)
    sys.stderr = StreamToLogger(logging.getLogger('STDERR'), logging.ERROR)

    print("在测试一下！")
    logging.info("这是info级别的日志输出")
