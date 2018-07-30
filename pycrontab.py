# -*- coding:utf-8 -*-
import os, time, uuid, platform
import logging
from multiprocessing import Process, Queue, freeze_support
from datetime import date, datetime, timedelta
from subprocess import Popen, PIPE

current_path = os.path.dirname(os.path.abspath(__file__))

decode = 'gb2312' if platform.system() == 'Windows' else 'utf-8'


########################################################################
class Job(object):
    """"""

    def __init__(self, script, crontab, *args):
        """Constructor"""
        self.script = script
        self.crontab = crontab
        self.add_time = datetime.now().replace(microsecond=0)
        self.run_batch_id = None
        self.next_time = None
        self.log_file = None
        self.log_file_timestamp = date.today().strftime('%Y%m%d')
        self.log_file_suffix = '-{timestamp}-{sequence}.log'
        self.log_file_sequence = 1
        self.logger = None
        self.status = 1 # -1:结束; 1：运行中
        self.year = self.crontab._year if self.crontab._year else 0
        self.month = self.crontab._month if self.crontab._month else 0
        self.day = self.crontab._day if self.crontab._day else 0
        self.week = self.crontab._week if self.crontab._week else 0
        self.hour = self.crontab._hour if self.crontab._hour else 0
        self.minute = self.crontab._minute if self.crontab._minute else 0
        self.second = self.crontab._second if self.crontab._second else 0
        self.gen_next_time()
        self.log()

    def gen_next_time(self):
        """"""
        if self.crontab._method == 'every':
            self.begin_time = self.crontab._begin_time if self.crontab._begin_time else self.add_time
            if self.next_time:
                self.next_time = self.next_time \
                             + timedelta(weeks=self.week) \
                             + timedelta(days=self.day) \
                             + timedelta(hours=self.hour) \
                             + timedelta(minutes=self.minute) \
                             + timedelta(seconds=self.second)
                if self.month > 0:
                    month = self.next_time.month + self.month
                    self.next_time.replace(year=self.next_time.year + month//12, month=month%12)

                if self.year > 0:
                    self.next_time.replace(year=self.next_time.year + self.year)
            else:
                if self.crontab._begin_time:
                    self.next_time = self.crontab._begin_time
                else:
                    self.next_time = self.add_time \
                                 + timedelta(weeks=self.week) \
                                 + timedelta(days=self.day) \
                                 + timedelta(hours=self.hour) \
                                 + timedelta(minutes=self.minute) \
                                 + timedelta(seconds=self.second)

            if self.crontab._end_time and self.next_time > self.crontab._end_time:
                self.status = -1

        else:
            self.next_time = datetime(year=self.year,
                                      month=self.month,
                                      day=self.day,
                                      hour=self.hour,
                                      minute=self.minute,
                                      second=self.second)
            if self.next_time < datetime.now():
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

    def __lt__(self, other):
        """"""
        return self.next_time < other.next_time

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

            formatter = logging.Formatter("%(asctime)s - %(filename)s - %(module)s - %(levelname)s - %(message)s")

            filehandler.setFormatter(formatter)
            consolehandler.setFormatter(formatter)

            logger.addHandler(filehandler)
            logger.addHandler(consolehandler)

        return logger

    def run(self):
        """"""
        self.logger = self._logger()
        self.logger.info('begin running script: {}'.format(self.script))
        try:
            cmd = 'python ' + self.script
            p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
            out, err = p.communicate()
            if err and p.returncode != 0:
                self.logger.error(
                    "The command finished with error: "
                    + err.decode(decode).replace('\r', '').rstrip('\n')
                )
        except Exception as e:
            self.logger.error(
                "The command finished with error: " + e.args[0] + e.args[1]
            )
        else:
            self.logger.info(
                "The stdout of the command: " \
                + out.decode(decode).replace('\r', '').rstrip('\n') \
                + ("\n" + err.decode(decode).replace('\r', '').rstrip('\n')).rstrip('\n')
            )
        finally:
            self.logger.info('finish running script: {}'.format(self.script))

    def __str__(self):
        return '<Job %r, method %r, next_time %s>' % (self.script, self.crontab._method, self.next_time)


########################################################################
class Crontab(object):
    """"""

    def __init__(self):
        """Constructor"""
        self._method = ''
        self._year = None
        self._month = None
        self._day = None
        self._week = None
        self._hour = None
        self._minute = None
        self._second = None
        self._begin_time = None
        self._end_time = None
        self._jobs = []

    def at(self):
        """一次性执行"""
        self._method = 'at'
        del Crontab.every
        del Crontab.begin
        del Crontab.end
        del Crontab.week
        del self._begin_time
        del self._end_time
        self._year = date.today().year
        self._month = date.today().month
        self._day = date.today().day
        return self

    def every(self):
        """循环执行"""
        self._method = 'every'
        del Crontab.at
        return self

    def begin(self, dtime):
        """开始时间，精确到秒"""
        if not isinstance(dtime, datetime):
            raise Exception("dtime参数必须为datetime类型")
        self._begin_time = dtime
        return self

    def end(self, dtime):
        """结束时间，精确到秒"""
        if not isinstance(dtime, datetime):
            raise Exception("btime参数必须为datetime类型")
        self._end_time = dtime
        return self

    def year(self, y):
        self._year = int(y)
        return self

    def month(self, m):
        """"""
        self._month = int(m)
        return self

    def day(self, d):
        """"""
        self._day = int(d)
        return self

    def week(self, w):
        """"""
        self._week = int(w)
        return self

    def hour(self, h):
        """"""
        self._hour = int(h)
        return self

    def minute(self, m):
        """"""
        self._minute = int(m)
        return self

    def second(self, s):
        """"""
        self._second = int(s)
        return self

    # def __getattr__(self, item):
    #     if (self._method == 'at' and item in ['every', 'begin', 'end']) \
    #             or (self._method == 'every' and item == 'at'):
    #         raise AttributeError("Job运行方式冲突: at不可以与 every、begin、end同时使用")
    #     return self

    def add(self, script):
        """"""
        if not os.path.exists(script):
            raise Exception('The script not found: {}'.format(script))
        j = Job(script, self)
        self._jobs.append(j)


    def loop(self, queue, debug):
        while True:
            run_batch_id = uuid.uuid1().hex
            now = datetime.now().replace(microsecond=0)
            for j in sorted(self._jobs):
                if debug:
                    j._logger(debug).info("{} ,now {}".format(str(j), str(now)))

                # 两秒钟的时间窗口，避免因为job太多导致错过部分job

                if (j.next_time + timedelta(seconds=2)) >= now >= j.next_time \
                        and run_batch_id != j.run_batch_id and j.status == 1:
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

