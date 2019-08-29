# -*- coding: utf-8 -*-
import requests
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import Select
import pandas as pd
import logging
import argparse
from selenium.common.exceptions import NoSuchElementException
import os
import pickle

path = ".\\Desktop"
logger = logging.getLogger('光化')

class Pickle:
    def __init__(self):
        self.site = '光化'
        
    def prepare_dump(self):
        dirname = os.path.join(path, self.site)
        os.makedirs(dirname, exist_ok = True)
        ts = time.time()
        filename = os.path.join(dirname, '{}.{}.pickle'.format(self.site, ts))
        return filename
    
    def syncbuf(self, obj):
        if len(obj) > 0:          
            output_file = self.prepare_dump()
            with open(output_file, 'wb') as fd:
                pickle.dump(obj, fd)
            logger.info("Convert to pickle file successfully")
        else:
            logger.warning("Fail to convert since the file is empty")
    
    def loadall(self, filename):
        pkl_list = []
        for pkl in filename:
            with open(pkl, "rb") as f:
                pkl_list.append(pickle.load(f))
        return pkl_list
            
class Crawl:
    def __init__(self): 
        self.baseurl = 'https://taqm.epa.gov.tw/taqm/tw/PamsDaily.aspx'
        self.uc = requests.Session()
        self.inf = ['site', 'para', 'date']
        self.id_ = ['select#ctl05_ddlSite option', 'select#ctl05_ddlParam option', 'select#ctl05_ddlYM option']
        self.all_df = [] #store all datas
        self.name = ['Date', 'Site', 'Parameters', 'value(ppbc)', 'label']
                    
    def get_info(self):
        r = self.uc.get(self.baseurl)
        sp = BeautifulSoup(r.text, 'lxml') 
        info = {}
        #get site、para、date value
        for i, j in zip(self.inf, self.id_):
            collect = []
            for item in sp.select(j):
                check = item['value']
                if '2017'  in check:
                    break
                collect.append(check)
            info.update({i : collect})
        return info
    
    def get_data(self):
        start = time.time()
        driver = webdriver.Chrome()
        info = self.get_info()
        info['date'].reverse()
        info['para'] = info['para'][:-1] #del n-Dodecane value
        driver.get(self.baseurl)
        for i in info['site']:
            driver.refresh()
            time.sleep(1.5)
            s1 = Select(driver.find_element_by_id('ctl05_ddlSite'))
            s1.select_by_value(i)
            time.sleep(1) #選完測站之後暫停1秒，以免抓不到元素
            one_site = []
            for j in info['para']:
                s2 = Select(driver.find_element_by_id('ctl05_ddlParam'))
                s2.select_by_value(j)
                time.sleep(1) #選完參數之後暫停1秒，以免抓不到元素
                try:
                    for k in info['date']:
                        s3 = Select(driver.find_element_by_id('ctl05_ddlYM'))
                        s3.select_by_value(k)
                        time.sleep(1.5)#選完日期之後暫停1.5秒，以免抓不到元素
                        sp = BeautifulSoup(driver.page_source, 'lxml')
                        null = []
                        data = []
                        for obj in sp.select('table.TABLE_G tr td'):
                            block = obj.text.strip()
                            if block:
                                null.append(block)
                            else:
                                null.append('有效')
                            if len(null) % 5 == 0:
                                data.append(pd.DataFrame(null).T)
                                null = []
                        try:
                            whole_data = pd.concat(data)
                            logger.info(whole_data)
                            one_site.append(whole_data)
                        except ValueError: #過濾空的2018 2019資料
                            continue
                except NoSuchElementException: #過濾沒有2018 2019資料的參數
                    continue #若無，跳到下一個參數
            logger.info("已消耗了 %s 秒" , (round(time.time() -start, 2)))
            if one_site:
                temp = pd.concat(one_site)
                temp.columns = self.name
                temp.index = range(0, len(temp))
                Pickle().syncbuf(temp) #一個測站爬完下載一次
                self.all_df.append(temp)   
        self.all_df = pd.concat(self.all_df)
        Pickle().syncbuf(self.all_df) #下載全部測站
        logger.info("共爬取 %s 筆資料", len(self.all_df.index))
        logger.info("共消耗了 %s 秒" , (round(time.time() - start, 2)))
        return self.all_df
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-d","--debug", help="getall ruten result", action="store_true")
    args = parser.parse_args()
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    all_df = Crawl().get_data()