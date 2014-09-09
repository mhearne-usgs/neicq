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

def getMostRecentPDE(cursor):
    query = 'select get_pde_from_ot(max(torigin)) from all_events_info_no_nph where iworkflowstatus = 8192'
    cursor.execute(query)
    pdestr = cursor.fetchone()[0]
    return pdestr

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

def emptyTable(cursor,db):
    cursor.execute('TRUNCATE table HDB_MONITOR.PDEHydra')
    db.commit()
    cursor.execute('SELECT count(idEvent) FROM HDB_MONITOR.PDEHydra')
    nrows = cursor.fetchone()[0]
    if nrows != 0:
        return False
    return True

def insertBaseRecords(cursor,db,tstart,tend):
    query = '''insert into HDB_MONITOR.PDEHydra(idEvent, OT, Lat, Lon, Depth, Mag, idBHPF, iPDENum)
    (select bhp.idEvent, tOrigin, dLat, dLon, dDepth, dPrefMag, bhp.idBHPellet, get_pde_from_ot(tOrigin)
    from all_bhpelletdescs bhp, 
	(
	select idevent, max(idbhpellet) idbhpellet from all_bhpelletdescs 
	where iworkflowstatus >= 8192*65536
	group by idevent
	) mbhp
    where bhp.idBHPellet = mbhp.idBHPellet
    and bhp.tOrigin between %i and %i
    )''' % (tstart,tend)
    query = query.replace('\n',' ')
    cursor.execute(query)
    db.commit()
    cursor.execute('SELECT count(idEvent) FROM HDB_MONITOR.PDEHydra')
    nrows = cursor.fetchone()[0]
    return nrows


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

    res = emptyTable(cursor,db)
    if not res:
        print 'Could not empty the PDE table!'
        sys.exit(1)
    insertBaseRecords(cursor,db,starttime,endtime)
    
    cursor.close()
    db.close()

if __name__ == '__main__':
    main()
    sys.exit(0)
    
