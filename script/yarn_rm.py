#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from __future__ import division
import requests
import urllib
import json
import hashlib
import time
import logging
import sys
import os
import re


class HadoopMonitor:
    def __init__(self):
        self.timeout = 10
        self.url_cluster ="http://10.75.57.21:8088/ws/v1/cluster/metrics"
        self.url_scheduler="http://10.75.57.21:8088/ws/v1/cluster/scheduler"
        self.queue=set(['root.default','root.hive','root.small','root.spark','root.full','root.production','root.hive','root.ads','root.hbase'])
    def process(self):
        h={
            "Accept": "application/json"
        }
        result=requests.get(self.url_cluster,headers=h,timeout=self.timeout).json()['clusterMetrics']
        for k,v in result.items():
            print ("%s:%s"%(k,v))
        print ("MemUsedPercent:%5.2f" % (result['allocatedMB']/result['totalMB']*100))
        print ("CpuUsedPercent:%5.2f" % (result['allocatedVirtualCores'] / result['totalVirtualCores'] * 100))
        result=requests.get(self.url_scheduler,headers=h,timeout=self.timeout).json()
        for item in result['scheduler']['schedulerInfo']['rootQueue']['childQueues']:
            if item['queueName'] in self.queue:
                print ("%sUsedPercent:%5.2f" % (item['queueName'],item['usedResources']['memory'] / item['maxResources']['memory'] * 100))
if __name__ == "__main__":
    h = HadoopMonitor()
    h.process()
