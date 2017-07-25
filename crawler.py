import requests
import re
from lxml import etree
from queue import Queue, Empty
from threading import Thread, Lock
import time
from random import random, choice
import urllib.parse
import logging


class Crawler():
    def __init__(self, verbose=False, n_workers=1):
        self._vacancies = []
        self._tasks = Queue()
        self._mutex = Lock()
        self._n_workers = n_workers
        self._running = False
        self._verbose = verbose
        self._alerts = []
        self._logger = logging.getLogger(__name__)


    def log(self, msg):
        if self._verbose:
            self._logger.critical(msg)


    def add_alert(self, alert):
        with self._mutex:
            self._alerts.append(alert)


    def add_to_queue(self, task):
        self.log("adding task to queue:")
        self.log("- url: %s\n- type: %s" % (task['url'], task['type']))
        if not task.get('data'):
            task['data'] = {}
        if task['data'].get('category'):
            self.log("- category: %s" % task['data']['category'])
        self._tasks.put(task)


    def abs_url(self, prefix, url):
        if re.match(r'http', url):
            return url
        else:
            return urllib.parse.urljoin(prefix, url)


    def join_cats(self, cat1, cat2):
        if cat1 and cat2:
            return " ->>> ".join(cat1.split(" ->>> ") + cat2.split(" ->>> "))
        elif cat1:
            return cat1
        elif cat2:
            return cat2
        else:
            return ""


    def level1(self, task):
        self.log("level1")
        doc = etree.HTML(task['body'])
        cats = doc.xpath("//div[@class='index-work-in-industry']//li")
        for cat in cats:
            url = self.abs_url(task['url'], cat.xpath("./a/@href")[0])
            name = str(cat.xpath("./a/text()")[0])
            data = task['data']
            data['category'] = name.strip()
            self.add_to_queue({'type': 'crawl', 'url': url, 'data': data})


    def level2(self, task):
        self.log("level2")
        xpath = "//div[@class='bloko-toggle bloko-toggle_expand']//div[contains(@class, 'catalog__item')]"
        doc = etree.HTML(task['body'])
        cats = doc.xpath(xpath)
        for cat in cats:
            url = self.abs_url(task['url'], cat.xpath("./a/@href")[0])
            print(cat.xpath("./a/text()")[0])
            name = cat.xpath("./a/text()")[0]
            data = task['data']
            data['category'] = self.join_cats(data.get('category'), name)
            self.add_to_queue({'type': 'crawl', 'url': url, 'data': data})


    def rules(self):
        return [
            {
                "pattern": ".*jobs.tut.by\/?$",
                "function": Crawler.level1
            },
            {
                "pattern": ".*jobs.tut.by\/catalog\/[\w-]+\/?$",
                "function": Crawler.level2
            }
        ]

    def UA(self):
        return choice([
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
            'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:54.0) Gecko/20100101 Firefox/54.0'
        ])


    def parse(self, task):
        self.log("parsing")
        try:
            rule_applied = False
            for rule in self.rules():
                if re.fullmatch(rule['pattern'], task['url']):
                    rule['function'](self, task)
                    rule_applied = True

            if not rule_applied:
                self.add_alert("no rules applied for url %s" % task['url'])

        except Exception as e:
            with self._mutex:
                self._logger.exception(e)
                self.add_alert(e)


    def crawl(self, task):
        self.log("crawling")
        try:
            response = requests.get(task['url'], headers={
                "User-Agent": self.UA(),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Encoding": "gzip, deflate, sdch, br",
                "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.6,en;q=0.4",
                "Host": "jobs.tut.by"
            })
            self.add_to_queue({
                "type": "parse",
                "url": task['url'],
                "body": response.text,
                "data": task.get('data') or {}
             })
        except Exception as e:
            with self._mutex:
                self._logger.exception(e)
                self.add_alert(e)


    def do_task(self):
        while self._running:
            task = None

            try:
                task = self._tasks.get_nowait()
                self.log("task got")
            except Empty:
                self.log("queue is empty, waiting")

            if task is not None:
                if task['type'] == 'parse':
                    self.parse(task)
                elif task['type'] == 'crawl':
                    self.crawl(task)
                    sleeptime = random() * 5 + 3
                    self.log("sleeping for %f secs" % sleeptime)
                    time.sleep(sleeptime)
                else:
                    self.add_alert('unknown task type: %s' % task['type'])

            else:
                sleeptime = 2
                self.log("sleeping for %f secs" % sleeptime)
                time.sleep(sleeptime)


    def start(self, task=None):
        self.log('starting')
        if task:
            self.add_to_queue(task)

        with self._mutex:
            if self._running:
                return

            self._threads = [Thread(target=self.do_task)
                             for i in range(self._n_workers)]

            self._running = True

        for thread in self._threads:
            thread.start()


    def stop(self):
        with self._mutex:
            if self._running:
                self._running = False

        for thread in self._threads:
            thread.join()


if __name__ == "__main__":
    c = Crawler(True, 4)
#    start_url = "https://jobs.tut.by"
    start_url = "https://jobs.tut.by/catalog/Avtomobilnyj-biznes"

    c.start({"type": "crawl", "url": start_url})
    while True:
        time.sleep(1)
