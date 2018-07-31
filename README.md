**一个Python实现的Crontab任务调度工具。支持Linux、Windows，支持Python2和Python3**

**使用方式**:

```
import os, datetime
from pycrontab import crontab, crontab_run

if __name__ == '__main__':
    # 每天17点30分运行一次script1
    script1 = '/opt/scrapy_weather.py'
    crontab.every('day').at(hour=17, minute=30).add(script)
    
    # 每5分钟运行一次script2
    script2 = '/opt/scrapy_news.py'
    crontab.every('minute').interval(5).add(script2)
    
    # 设置开始时间和结束时间
    script3 = '/opt/scrapy_goods.py'
    begin_time = datetime.datetime.strptime('2018-06-01 00:00:00', '%Y-%m-%d %H:%M:%S')
    end_time = datetime.datetime.strptime('2018-10-01 00:00:00', '%Y-%m-%d %H:%M:%S')
    crontab.every('minute').interval(5).begin(begin_time).end(end_time).add(script3)
    
    # 开始运行crontab, 默认debug=False
    crontab_run(debug = True)

```

> 可以使用supervisor守护该脚本的进程

