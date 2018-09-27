#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import requests
import urllib
import json
import hashlib
import time
import logging
import sys
import os
import re
import socket

metrics = {}


class Logger(object):
    def __init__(self, lpath):
        logger_name = "order.log"
        self.log = logging.getLogger(logger_name)
        self.log.setLevel(logging.DEBUG)
        log_path = "/tmp/%s" % (lpath)
        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.DEBUG)

        fmt = "%(asctime)-15s %(levelname)s %(filename)s %(lineno)d %(process)d %(message)s"
        datefmt = "%a %d %b %Y %H:%M:%S"
        formatter = logging.Formatter(fmt, datefmt)
        fh.setFormatter(formatter)
        self.log.addHandler(fh)

    def logger_error(self, m):
        self.log.error(m)

    def logger_info(self, m):
        self.log.info(m)


class HadoopMonitor(Logger):
    def __init__(self, conf):
        self.conf = conf
        self.timeout = 10
        Logger.__init__(self, 'hdfs_monitor.log')

    def recursive(self, text, match):
        try:
            key = match[0]
            if key.startswith('['):
                key = int(re.split('[\[\]]', key)[1])
            if len(match) <= 1:
                return text[key]
            else:
                return self.recursive(text[key], match[1:])
        except (TypeError, IndexError, KeyError, ValueError) as e:
            return None

    def findIp(self, appname):
        cmd = 'ark-query relation host -s app %s -a -j' % (appname)
        return json.loads(os.popen(cmd).readlines()[0].strip('\n'))

    def findAlreay(self, metrices, value):
        data = None
        with open("/export/Scripts/conf/tmp.json") as f:
            data = json.load(f)
            result = None
            if data and data.has_key(metrices):
                result = data[metrices]
                data[metrices] = value
                json.dump(data, f)
            else:
                result = 0
            f.close()
        return result

    def endResult(self, na, ips, node, bean):
        for m in na['name']:
            result = self.recursive(bean, m.split('.')[1:])
            m= '_'.join(m.split('.')[1:])
            print ("tags:cluster:%s,item:%s" % (self.cluster, m))
            if result is not None:
                print ('status:%5.2f' % (result))
            else:
                result = sys.maxint
                print ('status:%d' % (sys.maxint))
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)
            now = int(float(time.time()))
            # \nsearch.us.byhost.192_168_117_131.http_2xx.os.hits:10|c10sAvg
            data = "search.us.byhost.%s.hdfs.%s:%5.2f|c" % (self.cluster,m,result)
            print s.sendto(data, ("logtailer.monitor.weibo.com", 8333)),data


    def printResult(self, response, ips, node, nodes):
        for na in nodes['metrics']:
            flag = True
            if na["category"].endswith('-'):
                flag = False
            for bean in response.json()['beans']:
                if flag:
                    if na["category"] == bean['name']:
                        self.endResult(na, ips, node, bean)
                else:
                    if bean['name'].startswith(na['category']):
                        self.endResult(na, ips, node, bean)

    def process(self):
        with open(self.conf) as f:
            nodes = json.load(f)

            for node in nodes['nodeList']:
                response = None
                self.cluster = node['cluster']
                host = None
                try:
                    for ip in node["namenodeIp"]:
                        url = "http://%s:50070/jmx" % (ip)
                        response = requests.get(url, timeout=self.timeout)
                        host = '10.75.57.21'
                        if response.status_code == 200:
                            if '"State" : "active"' in response.text:
                                self.printResult(response, host, node, nodes)
                except:
                    self.logger_error('%s:%s' % (self.cluster, sys.exc_info()))


if __name__ == "__main__":
    h = HadoopMonitor(sys.argv[1])
    h.process()


