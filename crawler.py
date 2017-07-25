import sys
import requests
import re
from lxml import etree
from queue import Queue, Empty
from threading import Thread, Lock
import time
from random import random, choice
import urllib.parse
import logging
from copy import deepcopy
import hashlib
import csv


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
        self._visited = set()
        self._active_workers = [0 for i in range(n_workers)]


    def mark_working(self, worker_number):
        with self._mutex:
            self._active_workers[worker_number] = 1


    def mark_idle(self, worker_number):
        with self._mutex:
            self._active_workers[worker_number] = 0


    def is_working(self):
        result = False

        with self._mutex:
            if sum(self._active_workers) > 0 or not self._tasks.empty():
                result = True

        return result


    def log(self, msg):
        if self._verbose:
            self._logger.critical(msg)


    def add_alert(self, alert):
        with self._mutex:
            self._alerts.append(alert)


    def add_vacancy(self, vacancy):
        self.log(vacancy)
        if self._mutex:
            self._vacancies.append(vacancy)


    def add_to_queue(self, task):
        self.log("trying to add task to queue:")
        if task['type'] == 'crawl':
            md5 = hashlib.md5(task['url'].encode('utf-8')).hexdigest()
            if md5 in self._visited:
                self.log("visited, skipping task")
                return
            else:
                self._visited.add(md5)
                self.log("not visited, adding")


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


    def get_category(self, task):
        try:
            return task.get('data').get('category')
        except:
            return None


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
        doc = task['doc'] if task.get('doc') is not None else etree.HTML(task['body'])
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
        doc = task['doc'] if task.get('doc') is not None else etree.HTML(task['body'])
        cats = doc.xpath(xpath)
        for cat in cats:
            url = self.abs_url(task['url'], cat.xpath("./a/@href")[0])
            self.log(cat.xpath("./a/text()")[0])
            name = cat.xpath("./a/text()")[0]
            data = deepcopy(task['data'])
            data['category'] = self.join_cats(data.get('category'), name)
            self.add_to_queue({'type': 'crawl', 'url': url, 'data': data})


    def pagination(self, task):
        self.log("pagination")
        doc = task['doc'] if task.get('doc') is not None else etree.HTML(task['body'])
        next_page = self.abs_url(task['url'], doc.xpath("//a[@data-qa='pager-next']/@href")[0])
        self.add_to_queue({'type': 'crawl', 'url': next_page, 'data': task['data']})


    def vacancies_list(self, task):
        self.log("list")
        doc = task['doc'] if task.get('doc') is not None else etree.HTML(task['body'])
        vacancies = doc.xpath("//div[@class='search-result-item__head']/a/@href")
        for vacancy_url in vacancies:
            self.add_to_queue({'type': 'crawl', 'url': vacancy_url, 'data': task['data']})


    def extractor(self, task):
        self.log("extractor")
        doc = task['doc'] if task.get('doc') is not None else etree.HTML(task['body'])
        top_info = doc.xpath("//div[@class='b-vacancy-info']//td//text()")

        vacancy = {
            "title": doc.xpath("//h1[contains(@class, 'b-vacancy-title')]/text()")[0].strip(),
            "company": doc.xpath("//div[@class='companyname']//text()")[0].strip(),
            "salary": top_info[0].strip(),
            "location": " ".join(top_info[1:-1]).strip(),
            "experience": top_info[-1].strip(),
            "skills": " |||| ".join(doc.xpath("//span[@data-qa='skills-element']//text()")),
            "employment_type": doc.xpath("//span[@itemprop='employmentType']/text()")[0],
            "workhours": doc.xpath("//span[@itemprop='workHours']/text()")[0],
            "category": self.get_category(task)
        }

        self.add_vacancy(vacancy)


    def rules(self):
        return [
            {
                "pattern": ".*jobs.tut.by\/?$",
                "function": Crawler.level1
            },
            {
                "pattern": ".*jobs.tut.by\/catalog\/[\w-]+\/?$",
                "function": Crawler.level2
            },
            {
                "pattern": ".*jobs.tut.by\/catalog(\/[\w-]+){2}\/?(\/page.*)?$",
                "function": Crawler.pagination
            },
            {
                "pattern": ".*jobs.tut.by\/catalog(\/[\w-]+){2}\/?(\/page.*)?$",
                "function": Crawler.vacancies_list
            },
            {
                "pattern": ".*\/vacancy\/\d+",
                "function": Crawler.extractor
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
            task['doc'] = etree.HTML(task['body'])
        except:
            pass

        rule_applied = False
        for rule in self.rules():
            try:
                if re.fullmatch(rule['pattern'], task['url']):
                    rule['function'](self, task)
                    rule_applied = True
            except Exception as e:
                self.add_alert(e)

        if not rule_applied:
            self.add_alert("no rules applied for url %s" % task['url'])



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
            self.add_alert(e)


    def do_task(self, worker_number):
        while self._running:
            self.mark_working(worker_number)
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
                    sleeptime = random() * self._n_workers + 3
                    self.log("sleeping for %f secs" % sleeptime)
                    time.sleep(sleeptime)
                else:
                    self.add_alert('unknown task type: %s' % task['type'])

            else:
                self.mark_idle(worker_number)
                sleeptime = 2
                self.log("sleeping for %f secs" % sleeptime)
                time.sleep(sleeptime)

            self.mark_idle(worker_number)



    def start(self, task=None):
        self.log('starting')
        if task:
            self.add_to_queue(task)

        with self._mutex:
            if self._running:
                return

            self._threads = [Thread(target=self.do_task, args=(i,)) for i in range(self._n_workers)]

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
    start_url = sys.argv[1]
    outfile = sys.argv[2]

    c = Crawler(False, 8)
    c.start({"type": "crawl", "url": start_url})

    while c.is_working():
        print("active workers: [%s]" % ", ".join(list(map(str, c._active_workers))))
        time.sleep(1)

    c.stop()
    print(c._alerts)

    with open(outfile, 'w') as f:
        fields = ['category', 'title', 'company', 'salary', 'location', 'experience', 'employment_type', 'workhours',  'skills']
        writer = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
        writer.writeheader()
        writer.writerows(c._vacancies)
