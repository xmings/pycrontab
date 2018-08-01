# -*- coding:utf-8 -*-
import os, time, uuid, platform, json, codecs
import logging
from multiprocessing import Process, Queue, freeze_support
from datetime import date, datetime, timedelta
from subprocess import Popen, PIPE


__all__ = ['crontab', 'crontab_run']

current_path = os.path.dirname(os.path.abspath(__file__))

decode = 'gb2312' if platform.system() == 'Windows' else 'utf-8'


########################################################################
class Job(object):
    """"""

    def __init__(self, script, executor, crontab):
        """Constructor"""
        self.script = script
        self.executor = executor
        self.add_time = datetime.now().replace(microsecond=0)
        self.run_batch_id = None
        self.next_time = None
        self.log_file = None
        self.log_file_timestamp = date.today().strftime('%Y%m%d')
        self.log_file_suffix = '-{timestamp}-{sequence}.log'
        self.log_file_sequence = 1
        self.logger = None
        self.status = 1 # -1:结束; 1：运行中
        self.method = crontab._method
        self.year = crontab._year
        self.month = crontab._month
        self.day = crontab._day
        self.hour = crontab._hour
        self.minute = crontab._minute
        self.second = crontab._second
        self.granula = crontab._granula
        self.begin_time = crontab._begin_time
        self.end_time = crontab._end_time
        self.gen_next_time()
        self.log()

    def gen_next_time(self):
        """"""
        if self.method == 'fix-all':
            self.next_time = datetime(year=self.year,
                                      month=self.month,
                                      day=self.day,
                                      hour=self.hour,
                                      minute=self.minute,
                                      second=self.second)
            self.status = -1
        elif self.method == 'fix-part':
            now = datetime.now().replace(microsecond=0)
            if not self.next_time:
                if self.begin_time:
                    self.next_time = self.begin_time
                else:
                    self.next_time = self.add_time

            if self.granula == 'year':
                self.next_time = self.next_time.replace(month=self.month,
                                       day=self.day, hour=self.hour, minute=self.minute, second=self.second)
                if (self.begin_time and self.next_time < self.begin_time) or self.next_time < now:
                    self.next_time = self.next_time.replace(year=self.next_time.year + 1)
            elif self.granula == 'month':
                self.next_time = self.next_time.replace(day=self.day, hour=self.hour, minute=self.minute, second=self.second)
                if (self.begin_time and self.next_time < self.begin_time) or self.next_time < now:
                    if self.next_time.month == 12:
                        self.next_time = self.next_time.replace(year=self.next_time.year + 1, month=1)
                    else:
                        self.next_time = self.next_time.replace(month=self.next_time.month + 1)
            elif self.granula == 'day':
                self.next_time = self.next_time.replace(hour=self.hour, minute=self.minute, second=self.second)
                if (self.begin_time and self.next_time < self.begin_time) or self.next_time < now:
                    self.next_time += timedelta(days=1)
            elif self.granula == 'hour':
                self.next_time = self.next_time.replace(minute=self.minute, second=self.second)
                if (self.begin_time and self.next_time < self.begin_time) or self.next_time < now:
                    self.next_time += timedelta(hours=1)
            elif self.granula == 'minute':
                self.next_time = self.next_time.replace(second=self.second)
                if (self.begin_time and self.next_time < self.begin_time) or self.next_time < now:
                    self.next_time += timedelta(minutes=1)

            if self.end_time and self.next_time > self.end_time:
                self.status = -1

        else: # interval
            if not self.next_time:
                if self.begin_time:
                    self.next_time = self.begin_time
                else:
                    self.next_time = self.add_time
            else:
                if self.year:
                    self.next_time = self.next_time.replace(year=self.next_time.year+1)
                elif self.month:
                    if self.next_time.month == 12:
                        self.next_time = self.next_time.replace(year=self.next_time.year+1, month=1)
                    else:
                        self.next_time = self.next_time.replace(month=self.next_time.month+1)
                elif self.day:
                    self.next_time += timedelta(days=self.day)
                elif self.hour:
                    self.next_time += timedelta(hours=self.hour)
                elif self.minute:
                    self.next_time += timedelta(minutes=self.minute)
                elif self.second:
                    self.next_time += timedelta(seconds=self.second)


                if self.end_time and self.next_time > self.end_time:
                    self.status = -1

    def gen_log_sequence(self):
        # 计算日志大小
        log_file = self.log_file.format(timestamp=self.log_file_timestamp, sequence=self.log_file_sequence)
        if os.path.getsize(log_file) > self.log_size * 1024 * 1024:
            self.log_file_sequence += 1

    def log(self, path=None, prefix=None, size=None):
        """"""
        if path:
            self.log_path = path
        else:
            self.log_path = os.path.join(current_path, 'log')
            if not os.path.exists(self.log_path):
                os.mkdir(self.log_path)

        if prefix:
            self.log_file = os.path.join(self.log_path, str(prefix) + self.log_file_suffix)
        else:
            self.log_file = os.path.join(self.log_path,
                                         os.path.splitext(os.path.basename(self.script))[0] + self.log_file_suffix)

        if size:
            self.log_size = size
        else:
            self.log_size = 10

    def _logger(self, debug=False):
        """"""
        log_file = self.log_file.format(timestamp=self.log_file_timestamp, sequence=self.log_file_sequence)

        logger = logging.getLogger(log_file)
        logger.setLevel(logging.DEBUG)
        if not logger.handlers:
            filehandler = logging.FileHandler(log_file, encoding='utf-8')
            filehandler.setLevel(logging.DEBUG)

            consolehandler = logging.StreamHandler()
            consolehandler.setLevel(logging.DEBUG if debug else logging.ERROR)

            formatter = logging.Formatter("%(asctime)s - %(filename)s - %(levelname)s - %(message)s")

            filehandler.setFormatter(formatter)
            consolehandler.setFormatter(formatter)

            logger.addHandler(filehandler)
            logger.addHandler(consolehandler)

        return logger

    def run(self):
        """"""
        self.logger = self._logger()
        self.logger.info('start running script: {}'.format(self.script))
        try:
            cmd = '{} {}'.format(self.executor, self.script)
            p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
            out, err = p.communicate()
            if err or p.returncode != 0:
                self.logger.error(
                    "The command finished with error: \n"
                    + err.decode(decode).replace('\r', '').rstrip('\n')
                )
            else:
                self.logger.info(
                    "The stdout of the command: \n"
                    + out.decode(decode).replace('\r', '').rstrip('\n')
                )
        except Exception as e:
            self.logger.error(
                "The command finished with error: " + e.args[0] + e.args[1]
            )
        finally:
            self.logger.info('finish running script: {}'.format(self.script))

    def __lt__(self, other):
        """"""
        return self.next_time < other.next_time

    def __repr__(self):
        return str({
            "script": self.script,
            "status": self.status,
            "begin_time": self.begin_time,
            "end_time": self.end_time,
            "method": self.method,
            "granula": self.granula,
            "year": self.year,
            "month": self.month,
            "day": self.day,
            "hour": self.hour,
            "minute": self.minute,
            "second": self.second
        })

    def __str__(self):
        return '<Job %r, method %r, next_time %s, status %s>' % (self.script, self.method, self.next_time, self.status)


########################################################################
class Crontab(object):
    """"""
    _jobs = []
    job_config_file = os.path.join(current_path, 'jobs.conf')
    def __init__(self):
        """Constructor"""
        self._method = ''
        self._year = None
        self._month = None
        self._day = None
        self._hour = None
        self._minute = None
        self._second = None
        self._begin_time = None
        self._end_time = None
        self._interval = None
        self._granula = None
        self._granulalist = ['year', 'month', 'day', 'hour', 'minute', 'second']

    def every(self, granula='day'):
        if granula not in self._granulalist:
            raise Exception("granula必须在{}中".format(','.join(self._granulalist)))

        self._granula = granula
        return self

    def at(self, **kwargs):
        """定时间点"""
        if self._method:
            raise Exception("不可重用interval和at方法.")

        if not self._granula:
            assert len(self._granulalist) == 6
            for k in kwargs:
                if k not in self._granulalist:
                    raise Exception("{}必须在{}中".format(k,','.join(self._granulalist)))
                setattr(self, '_' + k, kwargs[k])
            self._method = 'fix-all'
            return self

        self._method = 'fix-part'

        if self._granula == 'year':
            self._month = kwargs.get('month', 1)
            self._day = kwargs.get('day', 1)
            self._hour = kwargs.get('hour', 0)
            self._minute = kwargs.get('minute', 0)
            self._second = kwargs.get('second', 0)

        elif self._granula == 'month':
            self._day = kwargs.get('day', 1)
            self._hour = kwargs.get('hour', 0)
            self._minute = kwargs.get('minute', 0)
            self._second = kwargs.get('second', 0)

        elif self._granula == 'day':
            self._hour = kwargs.get('hour', 0)
            self._minute = kwargs.get('minute', 0)
            self._second = kwargs.get('second', 0)

        elif self._granula == 'hour':
            self._minute = kwargs.get('minute', 0)
            self._second = kwargs.get('second', 0)

        elif self._granula == 'minute':
            self._second = kwargs.get('second', 0)

        elif self._granulalist == 'second':
            raise Exception("every('second')时不支持at,可使用interval!")

        return self

    def interval(self, num):
        """定间隔"""
        if self._method:
            raise Exception("不可重用interval和at方法.")
        if not self._granula:
            raise Exception("必须先使用every方法指定频率粒度")
        self._method = 'interval'
        setattr(self, '_' + self._granula, num)
        return self

    def begin(self, dtime):
        """开始时间，精确到秒"""
        if not isinstance(dtime, datetime):
            raise Exception("dtime参数必须为datetime类型")
        self._begin_time = dtime.replace(microsecond=0)
        return self

    def end(self, dtime):
        """结束时间，精确到秒"""
        if not isinstance(dtime, datetime):
            raise Exception("btime参数必须为datetime类型")
        self._end_time = dtime.replace(microsecond=0)
        return self

    def add(self, script, executor='python'):
        if not os.path.exists(script):
            raise Exception("未找到该脚本:{}".format(script))
        if os.path.splitext(script)[1].lower() != '.py' and executor.lower() =='python':
            raise Exception("必须提供正确的执行程序，如python, java, bash等")
        j = Job(script, executor, self)
        self._jobs.append(j)
        self.__init__()

    def __getstate__(self):
        return self._jobs

    def __setstate__(self, state):
        self._jobs = state

    def loop(self, queue, debug):
        with codecs.open(self.job_config_file, 'a', encoding='utf-8') as f:
            for j in self._jobs:
                json.dump(repr(j), f, ensure_ascii=False)
                f.write('\n')

        while True:
            run_batch_id = uuid.uuid1().hex
            now = datetime.now().replace(microsecond=0)
            for j in self._jobs:
                if j.status == -1:
                    self._jobs.remove(j)

            for j in sorted(self._jobs):
                if debug:
                    j._logger(debug).info("{}".format(str(j)))

                # 两秒钟的时间窗口，避免因为job太多导致错过部分job
                if (j.next_time + timedelta(seconds=2)) >= now >= j.next_time \
                        and run_batch_id != j.run_batch_id:
                    j.run_batch_id = run_batch_id
                    if debug:
                        j._logger(debug).info("put job into queue: {}".format(str(j)))
                    queue.put(j)
                    j.gen_next_time()
                    j.gen_log_sequence()

            time.sleep(1)

def first_runner(queue):
    while True:
        j = queue.get()
        j.run()


def second_runner(queue):
    while True:
        j = queue.get()
        j.run()


crontab = Crontab()

def crontab_run(debug=False):
    freeze_support()
    queue = Queue()
    ps = []
    p1 = Process(target=crontab.loop, args=(queue, debug))
    ps.append(p1)
    p2 = Process(target=first_runner, args=(queue,))
    ps.append(p2)
    p3 = Process(target=second_runner, args=(queue,))
    ps.append(p3)

    for p in ps:
        p.start()

    for p in ps:
        p.join()


if __name__ == '__main__':
    crontab_run()

