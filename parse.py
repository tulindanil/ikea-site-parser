#!/usr/bin/env python

from __future__ import unicode_literals

import urllib2, logging
import sqlite3
import os
import sys

from HTMLParser import HTMLParser

import signal
import sys

def signal_handler(signal, frame):
    
    logging.debug('You pressed Ctrl+C!')
    sys.exit(1)

class Item:
    
    fields = [('name', ''), ('title', ''), ('desc', ''), ('price', ''), ('serial', '')]

    def printItem(self):
        for x in range (0, len(self.fields)):
            print self.fields[x][1]

qty = 0

class productsAzLinkParser(HTMLParser):

    def __init__(self):
        HTMLParser.__init__(self)
        self.urls = []
        self.doesSpanHashtagBegin = 0

    def handle_starttag(self, tag, attrs):
        if tag == 'span' and attrs == [('class', 'productsAzLink')]:
            self.doesSpanHashtagBegin = 1

        if tag == 'a' and self.doesSpanHashtagBegin:
            self.urls.append(attrs[0][1])
    
    def handle_endtag(self, tag):
        if tag == 'span' and self.doesSpanHashtagBegin:
            self.doesSpanHashtagBegin = 0

class categoryParser(HTMLParser):

    def __init__(self):
        HTMLParser.__init__(self)
        self.itemURLs = []
        self.attrs = ['a', ('class', 'productLink')]

    def handle_starttag(self, tag, attrs):
        if tag == self.attrs[0] and len(attrs) >= 2:
            if attrs[1] == self.attrs[1]:
                self.itemURLs.append(attrs[0][1])


class itemParser(HTMLParser):

    item = Item()
    encoding = 'UTF-8'
    
    fetchMask = []
    attrs = [['div', ('id', 'name')], ['div', ('class', 'prodInfoRow')], ['div', ('id', 'type')], ['span', ('id', 'price1')], ['div', ('id', 'itemNumber')]]
    
    otherItemsFetching = 0
    otherItemsAttrs = ['select', [('class', 'dropdown'), ('id', 'dropAllAttributes'), ('name', 'partNumber'),  ('title', 'dropAllAttributes')]]
    otherItemsSerial = []
    
    def __init__(self):
        HTMLParser.__init__(self)
        self.fetchMask = []
        
        for x in range(0, len(self.item.fields)):
            self.fetchMask.append(0)
    
    def handle_starttag(self, tag, attrs):
        
        if tag == self.otherItemsAttrs[0] and attrs == self.otherItemsAttrs[1]:
        
            self.otherItemsFetching = 1
            self.otherItemsSerial = []
        
        elif self.otherItemsFetching == 0:
        
            for x in range(0, len(self.fetchMask)):
                if tag == self.attrs[x][0]:
                    if len(attrs) > 0:
                        if attrs[0] == self.attrs[x][1]:
                            self.fetchMask[x] = 1

        elif self.otherItemsFetching:
            
            if tag == 'option' and self.otherItemsFetching and len(attrs) == 1:
                self.otherItemsSerial.append(attrs[0][1])


    def handle_data(self, data):
        for x in range(0, len(self.fetchMask)):
            if self.fetchMask[x]:
                
                if self.item.fields[x][0] == 'price':
                    self.item.fields[x] = (self.item.fields[x][0], data.strip()[:-2].replace(unichr(160), ''))
                else:
                    self.item.fields[x] = (self.item.fields[x][0], data.strip().replace('\n', '').replace(chr(9), '').replace(chr(13), ''))

                self.fetchMask[x] = 0


    def handle_endtag(self, tag):
        if self.otherItemsFetching and tag == self.otherItemsAttrs[0]:
            self.otherItemsFetching = 0

        for x in range(0, len(self.fetchMask)):
            if self.fetchMask[x]:
                if tag == self.attrs[x][0]:
                    self.fetchMask[x] = 0

class Database():

    databaseName = 'data.sqlite3'

    connection = None
    cursor = None

    def __init__(self):
        
        self.connection = sqlite3.connect(databaseName)
        self.cursor = self.connection.cursor()

    def insertItem(self, item):
        
        sql = 'INSERT INTO UNIT VALUES('
        sql += item.fields[4][1].replace('.', '')
        sql += ", '"
        sql += item.fields[0][1]
        sql += "', '"
        sql += item.fields[1][1]
        sql += "', '"
        sql += item.fields[2][1]
        sql += "', "
        sql += item.fields[3][1];
        sql += ");"
        
        try:
        
            logging.debug(sql + '\n')
            self.cursor.execute(sql)
        
        except sqlite3.Error as e:

            logging.warning(sql + ' ' + e.args[0])

        try:
            
            self.connection.commit()
        
        except:
            
            logging.error('SQLITE COMMIT ISSUE')

    def doesNotContainItem(self, serial):
        
        self.cursor.execute('SELECT * FROM UNIT WHERE ID = ' + serial)
        return self.cursor.fetchone() == None


class WebWorker():
    
    def __init__(self):
        self.database = Database()
        self.allItemsAZUrl = 'http://www.ikea.com/ru/ru/catalog/productsaz'
        self.ikeaUrl = 'http://www.ikea.com'
        self.productUrl = '/ru/ru/catalog/products/'

    def catchDataAtUrl(self, url):
    
        try:
            response = urllib2.urlopen(url)
            data = response.read()
            return data.strip()
        except:
            logging.error('Can\'t parse: ' + url)
            return ''

    
    def processItemsAtIndex(self, index):

        parser = productsAzLinkParser()
        data = self.catchDataAtUrl(self.allItemsAZUrl + '/' + str(index))
        parser.feed(data)
        
        logging.info('Items at index ' + str(index) + ': ' + str(len(parser.urls)))
        
        for itemURL in parser.urls:
            if 'categories' in itemURL:
                logging.debug('Category :' + itemURL + '\n')
                self.processCategory(itemURL)
                logging.debug('End of category\n')
            else:
                self.processItemAtUrl(itemURL, 1)

    def processCategory(self, URL):
        
        data = self.catchDataAtUrl(self.ikeaUrl + URL)
        parser = categoryParser()
        parser.feed(data.decode('UTF-8'))
    
        for itemURL in parser.itemURLs:
            self.processItemAtUrl(itemURL, 1)

    def processItemAtUrl(self, url, recursive):

        logging.debug('Parsing an item:' + url)

        if os.fork() == 0:

            logging.basicConfig(filename = 'parser.log', format = '%(levelname)s:%(message)s', level = logging.INFO)

            parser = itemParser()
            data = self.catchDataAtUrl(self.ikeaUrl + url)
            
            parser.feed(data.decode('UTF-8'))
            self.database.insertItem(parser.item)
            
            global qty
            qty += 1
            
            if qty % 100 == 0:
                logging.info(str(qty) + '\n')
            
            if recursive == 1 and len(parser.otherItemsSerial):
                for x in parser.otherItemsSerial:
                    self.processItemAtUrl(self.productUrl + str(x), 0)

            sys.exit(0)


databaseName = 'data.sqlite3'

if __name__ == '__main__':

    signal.signal(signal.SIGINT, signal_handler)
    
    os.system('python init.py')
    logging.basicConfig(format = '%(levelname)s:%(message)s', level = logging.INFO)
    
    worker = WebWorker()

    for index in range(0, 30):
        worker.processItemsAtIndex(index)
