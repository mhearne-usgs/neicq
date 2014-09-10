#!/usr/bin/env python

#stdlib imports
import sys
from datetime import datetime
import os.path
import ConfigParser

#third party imports
import cx_Oracle

TIMEFMT = '%Y-%m-%d %H:%M:%S'
DEBUG = True

def getEventCount(cursor):
    query = 'select count(idEvent) from PDEHydra'
    cursor.execute(query)
    nevents = cursor.fetchone()[0]
    return nevents

def getPDERange(cursor,pdenumber):
    if DEBUG:
        pdemin = 1397600000
        pdemax = 1397610000
    else:
        query = 'select get_start_of_pde(pdenumber), get_end_of_pde(pdenumber) from dual'
        cursor.execute(query)
        row = cursor.fetchone()
        pdemin = row[0]
        pdemax = row[1]
    return (pdemin,pdemax)

def getMostRecentPDE(cursor):
    query = 'select get_pde_from_ot(max(torigin)) from all_events_info_no_nph where iworkflowstatus = 8192'
    cursor.execute(query)
    pdestr = cursor.fetchone()[0]
    return pdestr

def main():
    homedir = os.path.dirname(os.path.abspath(__file__)) #where is this script?
    lastfile = os.path.join(homedir,'lastprocessed.txt')
    if not os.path.isfile(lastfile):
        lastprocessed = 190001
    else:
        f = open(lastfile,'rt')
        lastprocessed = int(f.readline().strip())
        f.close()
    configfile = os.path.join(homedir,'config.ini')
    config = ConfigParser.ConfigParser()
    config.readfp(open(configfile))
    ip = config.get('DATABASE','ip')
    port = int(config.get('DATABASE','port'))
    user = config.get('DATABASE','user')
    password = config.get('DATABASE','password')
    sid = config.get('DATABASE','sid')
    tns = cx_Oracle.makedsn(ip,port,sid)
    db = cx_Oracle.connect(user,password,tns)
    cursor = db.cursor()
    pdenumber = getMostRecentPDE(cursor)
    if pdenumber <= lastprocessed:
        print 'Most recent PDE is not newer than the most recently processed.  Exiting.'
        sys.exit(0)
    starttime,endtime = getPDERange(cursor,pdenumber)
    startdate = datetime.utcfromtimestamp(starttime)
    enddate = datetime.utcfromtimestamp(endtime)

    try:
        rc = cursor.var(cx_Oracle.NUMBER)
        res = cursor.callproc('qa_do_quarterly_prep',[rc,starttime,endtime])
    except cx_Oracle.DatabaseError, exc:
        error, = exc.args
        print "Oracle-Error-Code:", error.code
        print "Oracle-Error-Message:", error.message

    if rc != 0:
        print 'Report prep procedure failed: Error code %i' % rc
    nevents = getEventCount(cursor)
    print '%i events created in pde report table.' % nevents
    cursor.close()
    db.close()

if __name__ == '__main__':
    homedir = os.path.dirname(os.path.abspath(__file__)) #where is this script?
    main()
    sys.exit(0)
    
