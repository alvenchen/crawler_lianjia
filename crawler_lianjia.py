#!/usr/bin/python
#-*- coding:utf-8 -*-

import re
import urllib2  
import sqlite3
import random
import threading
from threading import Timer
from bs4 import BeautifulSoup
import string
import time
import datetime
import json
import cookielib

import sys
reload(sys)
sys.setdefaultencoding("utf-8")

#import LianJiaLogIn


filename = 'cookie.txt'
cookie = cookielib.MozillaCookieJar()
print 'load cookie'
cookie.load('cookie.txt', ignore_discard=True, ignore_expires=True)

#Some User Agents

hds=[{'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:51.0) Gecko/20100101 Firefox/51.0'},\
{'User-Agent':'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
{'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
{'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36'},\
{'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; .NET4.0C; .NET4.0E; .NET CLR 2.0.50727; .NET CLR 3.0.30729; .NET CLR 3.5.30729; InfoPath.3; rv:11.0) like Gecko'},\
{'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\
{'User-Agent':'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\
{'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11'},\
{'User-Agent':'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11'},\
{'User-Agent': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)'}]

#hds=[{'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36'}]
regions=[u"罗湖区",u"福田区",u"南山区",u"盐田区",u"宝安区",u"龙岗区"]
#regions=[u"南山区"]
global communit_num
communit_num=0
lock = threading.Lock()


class SQLiteWraper(object):
    """
    数据库的一个小封装，更好的处理多线程写入
    """
    def __init__(self,path,command='',*args,**kwargs):  
        self.lock = threading.RLock() #锁  
        self.path = path #数据库连接参数  

        if command!='':
            conn=self.get_conn()
            cu=conn.cursor()
            cu.execute(command)

    def get_conn(self):  
        conn = sqlite3.connect(self.path)#,check_same_thread=False)  
        conn.text_factory=str
        return conn   

    def conn_close(self,conn=None):  
        conn.close()  

    def conn_trans(func):  
        def connection(self,*args,**kwargs):  
            self.lock.acquire()  
            conn = self.get_conn()  
            kwargs['conn'] = conn  
            rs = func(self,*args,**kwargs)  
            self.conn_close(conn)  
            self.lock.release()  
            return rs  
        return connection  

    @conn_trans    
    def execute(self,command,method_flag=0,conn=None):  
        cu = conn.cursor()
        try:
            if not method_flag:
                cu.execute(command)
            else:
                cu.execute(command[0],command[1])
            conn.commit()
        except sqlite3.IntegrityError,e:
            print e
            return -1
        except Exception, e:
            print e
            return -2
        return 0

    @conn_trans
    def fetchall(self,command='select name from xiaoqu order by name limit 2040, 40',conn=None):

        cu=conn.cursor()
        lists=[]
        try:
            cu.execute(command)
            lists=cu.fetchall()
        except Exception,e:
            print e
            pass
        return lists


def gen_xiaoqu_insert_command(info_dict):
    """
    生成小区数据库插入命令
    """
    info_list=[u'name',u'district',u'business_circle',u'house_module_url',u'build_time',u'metro_info']
    t=[]
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t=tuple(t)
    command=(r"insert into xiaoqu values(?,?,?,?,?,?)",t)#raw string
    return command


def gen_chengjiao_insert_command(info_dict):
    """
    生成成交记录数据库插入命令
    """
    info_list=[u'href',u'name',u'room',u'hall',u'size',u'direction',u'other_info',u'lift_info',u'deal_date',u'total_price',u'unit_price',u'floors',u'build_time']
    t=[]
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t=tuple(t)
    command=(r"insert into chengjiao values(?,?,?,?,?,?,?,?,?,?,?,?,?)",t)
    return command


def xiaoqu_spider(db_xq,url_page):
    """
    爬取页面链接中的小区信息
    """
    try:
        req = urllib2.Request(url_page,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urllib2.urlopen(req,timeout=10).read()
        plain_text=unicode(source_code)#,errors='ignore')   

        #TODO print
        #print 'xiaoqu_spider plain_text %s' %plain_text
        soup = BeautifulSoup(plain_text, "html.parser")
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        exit(-1)
    except Exception,e:
        print e
        exit(-1)

    #xiaoqu_list=soup.findAll('div',{'class':'info-panel'})
    xiaoqu_list=soup.findAll('div',{'class':'info'})

    for xq in xiaoqu_list:
        community={}
        community.update({u'name':xq.find('a').text})
        houseInfo = xq.find('div',{'class':'houseInfo'})
        house_module_url = houseInfo.find('a').get('href')
        position = soup.find('div',{'class':'positionInfo'})
        district = position.find('a', {'class':'district'}).get_text()
        business_circle = position.find('a', {'class':'bizcircle'}).get_text()
        build_time = position.get_text()
        metro_info = xq.find('div', {'class':'tagList'})
        if metro_info and metro_info.span :
            metro_info = metro_info.span.get_text()
        else :
            metro_info = ''
            #print 'no metro : %s' %community["name"]

        build_time = build_time[build_time.find('/')+1:].strip()
        build_time = re.findall(r'(\w*[0-9]+)\w*',build_time)
        if len(build_time) > 0 :
            build_time = build_time[0]
            build_time = string.atoi(build_time,10)
        else:
            build_time = 0
        community.update({u'house_module_url':house_module_url})
        community.update({u'district':district})
        community.update({u'business_circle':business_circle})
        community.update({u'build_time':build_time})
        community.update({u'metro_info':metro_info})
        
        command=gen_xiaoqu_insert_command(community)
        #TODO print
        #print 'xiaoqu list insert command : %s' %(command,) #tuple print
        db_xq.execute(command,1)
        global communit_num
        communit_num+=1


def do_xiaoqu_spider(db_xq,region):
    """
    爬取大区域中的所有小区信息
    """
    url=u"http://sz.lianjia.com/xiaoqu/rs"+region+"/"
    try:
        
        req = urllib2.Request(url,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urllib2.urlopen(req,timeout=5).read()
        plain_text=unicode(source_code)#,errors='ignore')   
        soup = BeautifulSoup(plain_text, "html.parser")
        

    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        return
    except Exception,e:
        print e
        return
    d="d="+soup.find('div',{'class':'page-box house-lst-page-box'}).get('page-data')
    exec(d)
    total_pages=d['totalPage']

    threads=[]
    for i in range(total_pages):
        url_page=u"http://sz.lianjia.com/xiaoqu/pg%drs%s/" % (i+1,region)
        t=threading.Thread(target=xiaoqu_spider,args=(db_xq,url_page))
        threads.append(t)
        
        #TODO add print
        print 'url_page:%s' %url_page
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    global communit_num
    print u"爬下了 %s %d 小区信息" % (region,communit_num)


def chengjiao_spider(db_cj,url_page):
    """
    爬取页面链接中的成交记录
    """
    print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    print url_page
    try:
        '''
        req = urllib2.Request(url_page,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urllib2.urlopen(req,timeout=10).read()
        plain_text=unicode(source_code)#,errors='ignore') 
        '''

        req = urllib2.Request(url_page,headers=hds[random.randint(0,len(hds)-1)])
        opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cookie))
        response = opener.open(req)
        plain_text=unicode(response.read())#,errors='ignore')

        #print plain_text

        soup = BeautifulSoup(plain_text, "html.parser")
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        exception_write('chengjiao_spider',url_page)
        return
    except Exception,e:
        print e
        exception_write('chengjiao_spider',url_page)
        return

    cj_list=soup.findAll('div',{'class':'info'})
    if not cj_list:
        print 'no class:info'
    #print cj_list
    for cj in cj_list:
        info_dict={}
        a=cj.find('a')
        if not a:
            continue
        href = a.attrs['href']
        brief = a.text.split()
        name = brief[0]
        type = brief[1]
        size = brief[2]

        type = re.findall(r'(\w*[0-9]+)\w*',type)
        if len(type) < 1:
            print '%s : type error' %url_page
            continue

        room = string.atoi(type[0],10)
        hall = string.atoi(type[1],10)
        size = re.findall(r'(\w*[0-9]+)\w*',size)
        size = string.atoi(size[0],10)

        houseInfo = cj.find('div', {'class':'houseInfo'})
        if houseInfo:
            houseInfo = houseInfo.text.split('|')
        else:
            houseInfo=[]
            houseInfo[0]=''
            houseInfo[1]=''
            houseInfo[2]=''

        dealDate = cj.find('div', {'class':'dealDate'})
        dealDate = dealDate.text

        positionInfo = cj.find('div', {'class':'positionInfo'})
        positionInfo = positionInfo.text.split(' ')
        floors = positionInfo[0]
        year = positionInfo[1]
        floors = re.findall(r'(\w*[0-9]+)\w*',floors)
        year = re.findall(r'(\w*[0-9]+)\w*',year)
        if len(floors)>0:
            floors = string.atoi(floors[0],10)
        else:
            floors = 0
        if len(year)>0:
            year = string.atoi(year[0],10)
        else:
            year = 0

        totalPrice = cj.find('div', {'class':'totalPrice'}).span.text
        unitPrice = cj.find('div', {'class':'unitPrice'}).span.text
        
        info_dict.update({u'href': href})
        info_dict.update({u'name': name})
        info_dict.update({u'room': room})
        info_dict.update({u'hall': hall})
        info_dict.update({u'size': size})

        info_dict.update({u'direction': houseInfo[0]})
        info_dict.update({u'other_info': houseInfo[1]})
        if len(houseInfo)>2:
            info_dict.update({u'lift_info': houseInfo[2]})
        else:
            info_dict.update({u'lift_info': ''})

        dealDate = datetime.datetime.strptime(dealDate, '%Y.%m.%d')
        info_dict.update({u'deal_date': dealDate})
        info_dict.update({u'total_price': totalPrice})
        info_dict.update({u'unit_price': unitPrice})
        info_dict.update({u'floors': floors})
        info_dict.update({u'build_time': year})

        print info_dict
        #print json.dumps(info_dict, encoding="UTF-8", ensure_ascii=False)

        command=gen_chengjiao_insert_command(info_dict)
        print command
        db_cj.execute(command,1)


def xiaoqu_chengjiao_spider(db_cj,xq_name):
    
    url=u"http://sz.lianjia.com/chengjiao/rs"+urllib2.quote(xq_name)+"/"
    #print url

    try:
        req = urllib2.Request(url,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urllib2.urlopen(req,timeout=10).read()
        plain_text=unicode(source_code)#,errors='ignore')   
        soup = BeautifulSoup(plain_text,"html.parser")
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        exception_write('xiaoqu_chengjiao_spider',xq_name)
        return
    except Exception,e:
        print e
        exception_write('xiaoqu_chengjiao_spider',xq_name)
        return
    content=soup.find('div',{'class':'page-box house-lst-page-box'})
    total_pages=0
    if content:
        d="d="+content.get('page-data')
        exec(d)
        total_pages=d['totalPage']

    '''
    threads=[]
    for i in range(total_pages+1):
        url_page=u"http://sz.lianjia.com/chengjiao/pg%drs%s/" % (i+1,urllib2.quote(xq_name))
        t=threading.Thread(target=chengjiao_spider,args=(db_cj,url_page))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    '''
    for i in range(total_pages+1):
        url_page=u"http://sz.lianjia.com/chengjiao/pg%drs%s/" % (i+1,urllib2.quote(xq_name))
        chengjiao_spider(db_cj, url_page)
        time.sleep(11)


def do_xiaoqu_chengjiao_spider(db_xq,db_cj):
    
    count=0
    xq_list=db_xq.fetchall()
    for xq in xq_list:
        xiaoqu_chengjiao_spider(db_cj,xq[0])
        count+=1
        print 'have spidered %d xiaoqu' % count
    print 'done'


def exception_write(fun_name,url):
    
    lock.acquire()
    f = open('log.txt','a')
    line="%s %s\n" % (fun_name,url)
    f.write(line)
    f.close()
    lock.release()


def exception_read():
    
    lock.acquire()
    f=open('log.txt','r')
    lines=f.readlines()
    f.close()
    f=open('log.txt','w')
    f.truncate()
    f.close()
    lock.release()
    return lines


def exception_spider(db_cj):
    
    count=0
    excep_list=exception_read()
    while excep_list:
        for excep in excep_list:
            excep=excep.strip()
            if excep=="":
                continue
            excep_name,url=excep.split(" ",1)
            if excep_name=="chengjiao_spider":
                chengjiao_spider(db_cj,url)
                count+=1
            elif excep_name=="xiaoqu_chengjiao_spider":
                xiaoqu_chengjiao_spider(db_cj,url)
                count+=1
            else:
                print "wrong format"
            print "have spidered %d exception url" % count
        excep_list=exception_read()
    print 'all done ^_^'



if __name__=="__main__":
    command="create table if not exists xiaoqu (name TEXT primary key UNIQUE, district TEXT, economic_circle TEXT, style TEXT, year INTEGER, metro_info TEXT)"
    db_xq=SQLiteWraper('lianjia_community.db',command)

    command="create table if not exists chengjiao (href TEXT primary key UNIQUE, name TEXT, room INTEGER, \
        hall INTEGER, size INTEGER, direction TEXT, other_info TEXT, lift_info TEXT, deal_date date, \
        total_price INTEGER, unit_price INTEGER, floors INTEGER, build_time INTEGER)"
    db_cj=SQLiteWraper('lianjia_chengjiao.db',command)

    #爬下所有的小区信息
    #for region in regions:
        #do_xiaoqu_spider(db_xq,region)

    #爬下所有小区里的成交信息
    do_xiaoqu_chengjiao_spider(db_xq,db_cj)

    #重新爬取爬取异常的链接
    #exception_spider(db_cj)
