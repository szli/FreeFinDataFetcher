# -*- coding: utf-8 -*-
"""
Created on Sun Jan 27 14:41:08 2013

@author: Shizheng Li
"""

#Finance Data Fetcher

import urllib2
import datetime
import os.path
import os
import time
import optparse 
import sys


DATA_ROOT_DIR = './../DailyData/Raw/'

#Directory names for each 
GOOGLE_DIR = 'GOOGLE'
YAHOO_DIR='YAHOO'
NETFONDS_DIR='NETFONDS'

LIST_TO_FETCH = './DataConfig/SP500SYMWikiJan252013.txt'

RAW_SUFFIX='.txt' # The suffix of the filename, AAPL.txt
ERROR_SUFFIX='.err' # When error happens, the content from the web page is saved to ticker.ERR_SUFFIX


class fetcher_base:
    def __init__(self):
        self.cache= ''

    def save_today(self, fn):
        tstr = str(datetime.date.today())
        save_d = os.path.join(DATA_ROOT_DIR, tstr)
        if not os.path.exists(save_d):
            os.mkdir(save_d)
        save_d = os.path.join(save_d, self.name().upper())
        if not os.path.exists(save_d):
            os.mkdir(save_d)
            
        fn = os.path.join(save_d, fn)    
        f = open(fn,'w')
        f.write(self.cache)
    

class google_fetcher(fetcher_base):
    
    def __init__(self, lookup_ex_code = False, sym_map_fn = None):
        fetcher_base.__init__(self)
        self.lookup_ex_code = lookup_ex_code
        self.EX_CODE = {'NASDAQ':'O', 'NYSE':'N', 'AMEX':'A'}
        if lookup_ex_code:
            self.sym_ex_map = {}
            self.sym_map_fn = sym_map_fn
        
    
    def fetch_today(self, sym):
        self.cache = urllib2.urlopen('http://www.google.com/finance/getprices?i=60&p=1d&f=d,o,h,l,c,v&df=cpct&q='+sym).read()
        if self.lookup_ex_code:
            line = self.cache[:self.cache.find('\n')]
            try:
                ex = line.split('%3D')[1]            
            except IndexError:
                print "Index Error in Sym [%s], the line is %s " % (sym, line)
                pass
            
            print sym,ex, self.EX_CODE[ex]
            self.sym_ex_map[sym] = self.EX_CODE[ex]
    
    def save_sym_ex_map(self):
        if self.lookup_ex_code:
            f = open(self.sym_map_fn,'w')
            f.write("#Symbol to exchange code from Google Finance, file generated at:%s\n" % datetime.date.today())
            for sym in sorted(self.sym_ex_map.keys()):
                f.write('%s %s\n' % (sym, self.sym_ex_map[sym]))
            f.close()
    def name(self):
        return "google"

class yahoo_fetcher(fetcher_base):    
    def fetch_today(self, sym):
        self.cache = urllib2.urlopen('http://chartapi.finance.yahoo.com/instrument/1.0/%s/chartdata;type=quote;range=1d/csv' % sym).read()
    def name(self):
        return "yahoo"
    
    
class netfonds_base(fetcher_base):
    def __init__(self, sym_map_fn):
        fetcher_base.__init__(self)
        f = open(sym_map_fn)
        self.sym_ex_map = {}
        for line in f:
            if line.strip(' \t')[0] != '#':            
                s_c = line.strip('\n\r').split(' ')
                self.sym_ex_map[s_c[0]] = s_c[1]        
        
    def get_exchange_code(self,sym):
        return self.sym_ex_map[sym]
        
    @classmethod
    def get_date_rep(cls,d):
        s  = str(d.year)
        s += str(d.month) if d.month >= 10 else '0' + str(d.month)
        s += str(d.day) if d.day >= 10 else '0' + str(d.day)
        return s

    
    
    
class netfonds_trade_fetcher(netfonds_base):
    def name(self):
        return "netfonds_trade"
        
    def fetch_today(self, sym):
        import datetime
        today_str = self.get_date_rep(datetime.date.today())
        ex_code = self.get_exchange_code(sym)
        url = 'http://hopey.netfonds.no/tradedump.php?date=%s&paper=%s.%s&csv_format=csv' % (today_str, sym, ex_code)
        #print "URL:",url
        self.cache =  urllib2.urlopen(url).read()


class netfonds_quote_fetcher(netfonds_base):
    def name(self):
        return "netfonds_quote"

    def fetch_today(self, sym):
        import datetime
        today_str = self.get_date_rep(datetime.date.today())
        ex_code = self.get_exchange_code(sym)
        url = 'http://hopey.netfonds.no/posdump.php?date=%s&paper=%s.%s&csv_format=csv' % (today_str, sym, ex_code)
        self.cache =  urllib2.urlopen(url).read()
        

class daily_fetch:

    def verboseDisp(self, mess):
        if self.verbose:
            print mess
            sys.stdout.flush()

    
    def __init__(self, sym_list_fn, verbose = False):        
        self.sym_list = [ line.strip('\n\r') for line in open(sym_list_fn)]        
        self.sym_list_name = sym_list_fn[:sym_list_fn.rfind('.')]
        self.err_log = "List %s" % self.sym_list_name
        self.main_log = "List %s" % self.sym_list_name
        self.verbose = verbose
        
    def fetch_goog(self):
        goog = google_fetcher(True, self.sym_list_name + '_ex_map.txt')
        for sym in self.sym_list:
            print "Fetching [%s] ..." % sym
            goog.fetch_today(sym)
        goog.save_sym_ex_map()
        
    def log(self):
        tstr = str(datetime.date.today())
        save_d = os.path.join(DATA_ROOT_DIR, tstr)
        if not os.path.exists(save_d):
            os.mkdir(save_d)
        f = open(os.path.join(save_d, 'daily_fetch_err.log'),'a')        
        f.write(self.err_log)
        f.close()
        f = open(os.path.join(save_d, 'daily_fetch_succ.log'),'a')
        f.write(self.main_log)
        f.close()
        
                
    def fetch_today_all(self):        
        fetchers = [google_fetcher(), yahoo_fetcher(), netfonds_trade_fetcher(self.sym_list_name + '_ex_map.txt'), netfonds_quote_fetcher(self.sym_list_name + '_ex_map.txt') ]
        any_err = False

        for fet in fetchers:
            start_mess = "Start fetcher %s at %s\n" % (fet.name(), time.asctime(time.localtime()))
            self.main_log = start_mess
            self.verboseDisp(start_mess)
                
            for sym in self.sym_list:            
                err = False
                try:
                    self.verboseDisp("Fetching [%s] from %s" % (sym, fet.name()))
                    fet.fetch_today(sym)                    
                except:
                    err = True
                    err_m = "Error when fetching [%s] from %s\n" % (sym, fet.name())                    
                    self.err_log += err_m
                    self.verboseDisp(err_m)
                    #raise
                
                if not err:                    
                    try:                
                        fet.save_today(sym + RAW_SUFFIX)
                    except:
                        err = True
                        err_m = "Error when saving [%s] from %s\n" % (sym, fet.name())
                        self.err_log += err_m
                        self.verboseDisp(err_m)
                else:
                    fet.save_today(sym + ERROR_SUFFIX)
                    
                    
                if not err:
                    succ_m = "Successfully processed [%s] from %s\n" % (sym, fet.name())
                    self.main_log += succ_m
                    self.verboseDisp(succ_m)
                    
                else:
                    any_err = True
                    fail_m = "Fail processing [%s] from %s\n" % (sym, fet.name())
                    self.main_log += fail_m
                    self.verboseDisp(fail_m)
                    
            end_mess = "Fetcher %s finished st %s\n" % (fet.name(), time.asctime(time.localtime()))                    
            self.verboseDisp(end_mess)
            self.main_log += end_mess
        
        self.log()
        return any_err
        
    def isTradingToday(self):
        #Currently only consider business days, not holidays
        return datetime.date.today().isoweekday() in range(1,6)
        
    #def compressToday(self):
                    
    def today_cron_job(self):
        import time
        print  "List %s Job started: " % self.sym_list_name, time.asctime(time.localtime())
        
        if not self.isTradingToday():
            print "Today is not a trading day. Finish. "
            return
        
        any_err = self.fetch_today_all()    
        if not any_err:
            print "Job succesful"
        else:
            print "Job has error!"
        
        print "List %s Job finished: " % self.sym_list_name, time.asctime(time.localtime())
        

def main():
    p = optparse.OptionParser()
    p.add_option("-v", "--verbose", dest = "verbose", action = "store_true", default = False)
    (opt, args) = p.parse_args()
    

    df = daily_fetch(LIST_TO_FETCH, opt.verbose)
    df.today_cron_job()
        
                
    
if __name__ == '__main__':
    main()

            
        
