#!/usr/local/bin/python3
import os
import time
import datetime
import pytz
import numpy
import random
import gzip
import zipfile
import sys
import argparse
from faker import Faker
from random import randrange

#Todo:
# Write generator for different log formats (Common Log, Apache Error log etc)

class switch(object):
    def __init__(self, value):
        self.value = value
        self.fall = False

    def __iter__(self):
        """Return the match method once, then stop"""
        yield self.match
        raise StopIteration

    def match(self, *args):
        """Indicate whether or not to enter a case suite"""
        if self.fall or not args:
            return True
        elif self.value in args: # changed for v1.5, see below
            self.fall = True
            return True
        else:
            return False

parser = argparse.ArgumentParser(__file__, description="Fake Apache Log Generator")
parser.add_argument("--output", "-o", dest='output_type', help="Write to a Log file, a gzip file or to STDOUT", choices=['LOG','GZ','CONSOLE'] )
parser.add_argument("--num", "-n", dest='num_lines', help="Number of lines to generate (0 for infinite)", type=int, default=1)
parser.add_argument("--prefix", "-p", dest='file_prefix', help="Prefix the output file name", type=str)
parser.add_argument("--anomalies", "-a", dest='data_anomalies', help='Mixes anomalous data into the number of lines generated', type=bool, default=0);

args = parser.parse_args()

log_lines = args.num_lines
file_prefix = args.file_prefix
output_type = args.output_type
anomalies = args.data_anomalies

faker = Faker()

timestr = time.strftime("%Y%m%d-%H%M%S")
otime = datetime.datetime.now()
logName = 'verb_single_feature_access_log_'
dest_dir = os.path.join('..', 'artificialLogs')

if anomalies == 0:
    outFileName = logName + timestr + '.log'
elif anomalies == 1:
    outFileName = logName + timestr + '--anomaly.log'

if file_prefix:
    outFileName = file_prefix + '_' + outFileName

for case in switch(output_type):
    if case('LOG'):
        f = open(os.path.join(dest_dir, outFileName),'w')
        break
    if case('GZ'):
        f = gzip.open(outFileName+'.gz','w')
        break
    if case('CONSOLE'): pass
    if case():
        f = sys.stdout

response=["200","404","500","301"]

verb=["GET","POST","DELETE","PUT", "BADVERB"]

resources=["/list","/wp-content","/wp-admin","/explore","/search/tag/list","/app/main/posts","/posts/posts/explore","/apps/cart.jsp?appID="]

ualist = [faker.firefox, faker.chrome, faker.safari, faker.internet_explorer, faker.opera]

flag = True
while (flag):
    increment = datetime.timedelta(seconds=random.randint(30,300))
    otime += increment

    ip = faker.ipv4()
    dt = otime.strftime('%d/%b/%Y:%H:%M:%S')
    tz = datetime.datetime.now(pytz.timezone('UTC')).strftime('%z')
    if anomalies == 0:
        vrb = numpy.random.choice(verb,p=[0.6,0.1,0.1,0.2, 0.0])
    elif anomalies == 1:
        vrb = numpy.random.choice(verb,p=[0.4,0.1,0.1,0.2, 0.2])

    uri = random.choice(resources)
    if uri.find("apps")>0:
        uri += repr(random.randint(1000,10000))

    resp = numpy.random.choice(response,p=[0.9,0.04,0.02,0.04])
    byt = int(random.gauss(5000,50))
    referer = faker.uri()
    useragent = numpy.random.choice(ualist,p=[0.5,0.3,0.1,0.05,0.05] )()
    f.write('%s - - [%s %s] "%s %s HTTP/1.0" %s %s "%s" "%s"\n' % (ip,dt,tz,vrb,uri,resp,byt,referer,useragent))

    log_lines = log_lines - 1
    flag = False if log_lines == 0 else True