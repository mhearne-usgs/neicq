#!/usr/bin/env python

#stdlib imports
import sys
from datetime import datetime
import os.path
import ConfigParser
import re

#third party imports
import cx_Oracle

#local imports
import neicq

TIMEFMT = '%Y-%m-%d %H:%M:%S'
DEBUG = False

QUARTERS = {'Q1':(1,13),'Q2':(14,26),'Q3':(27,39),'Q4':(40,52)}

MONSTER_QUERY = '''select /*+ FIRST_ROWS(1) */
 nvl(substr(el.huidevent,1,8), 'NaN')  || ',' EventCode, nvl(substr(el.teventcreated - aoInitial.torigin, 1, 10), 'NaN')  || ',' tDetectLatency,
   	nvl(substr(p.OT,1,13), 'NaN') || ',' tOriginPDE, nvl(substr(round(p.Mag,1),1,10), 'NaN') || ','  MagPDE,
   	nvl(substr(p.Lat,1,10), 'NaN') || ','  dLatPDE, nvl(substr(p.Lon,1,10), 'NaN') || ','  dLonPDE,
   	nvl(substr(p.Depth,1,11), 'NaN') || ','  dDepthPDE, nvl(substr(AOHF.iUsedPh,1,10), 'NaN') || ','  iUsedPhPDE,
   	nvl(substr(InitRel.tOrigin,1,13), 'NaN') || ','  tOriginInitial, nvl(substr(round(InitRel.dPrefMag,1),1,10), 'NaN') || ','  MagInitial,
   	nvl(substr(InitRel.dLat,1,10), 'NaN') || ','  dLatInitial, nvl(substr(InitRel.dLon,1,10), 'NaN') || ','  dLonInitial,
   	nvl(substr(InitRel.dDepth,1,3), 'NaN') || ','  dDepthInitial,
   	nvl(substr(AOHQR.dERRMAXHKM,1,4), 'NaN') || ','  dErrMaxHInitial, nvl(substr(AOHQR.dERRMAXZKM,1,4), 'NaN') || ','  dErrMaxZInitial,
   	substr('UNKNOWN',1,20) || ','  scheduled_analyst,
   	nvl(substr(Name_Hash(ECMin.sClaimAuthor),1,20), '<NONE>') || ','  Analyst1stClaim, nvl(substr(ECMin.tOn-p.OT,1,10), 'NaN') || ','  t1stClaimed,
   	nvl(substr(Name_Hash(ECMax.sClaimAuthor),1,20), '<NONE>') || ','  AnalystLastClaim, nvl(substr(ECMax.tOff-p.OT,1,10), 'NaN') || ','  tLastUnclaim,
   	nvl(substr(Name_Hash(EPQR.sName),1,20), 'NaN') || ','  AnalystFirstPub,
   	nvl(decode(InitRel.idBHPellet, InitRel.idBHPellet, 'TRUE','MAYBE'), 'FALSE') || ','  QuickRev,
   	nvl(substr(to_char(EPQR.tPassEntryCreated-p.OT),1,10), 'NaN') || ','  tFirstPub,
   	nvl(substr(Name_Hash(EPMTOnly.sName),1,20), 'NaN') || ','  Analyst2ndPub,
   	nvl(decode(BHPMTOnly.idBHPellet, BHPMTOnly.idBHPellet, 'TRUE','MAYBE'), 'FALSE') || ','  Q2uickRev,
   	nvl(substr(to_char(EPMTOnly.tPassEntryCreated-p.OT),1,10), 'NaN') || ','  tSecondPub,
   	nvl(AOExt.sInstCode, '<NONE>') || ','  sInstFirstExtPub, nvl(substr(AOExt.tinserted-p.OT,1,10), 'NaN') || ',' tFirstExtPub,
   	nvl(substr(aoATX.tinserted-p.OT,1,10), 'NaN') || ',' tFirstATWCPub,
   	nvl(substr(aoPTX.tinserted-p.OT,1,10), 'NaN') || ',' tFirstPTWCPub,
   	nvl(substr(aoATI.tinserted-p.OT,1,10), 'NaN') || ',' tFirstATWCIntPub,
   	nvl(substr(aoPTI.tinserted-p.OT,1,10), 'NaN') || ',' tFirstPTWCIntPub,
   	decode(p.idEvtShadow, NULL,'FALSE','TRUE') || ','  InShadow,
   	nvl(substr(to_char(BHPResp.tOutput - p.OT),1,10),'<NEVER>') || ','  tBecameResp,
   	substr(ctime(p.OT),0,19) || ','  Event_Date_Time,
   	nvl(substr(el.dPrefMag,1,10),'NaN') || ','  PrefMagCurrent,
   	nvl(substr(el.dLat,1,10), 'NaN') || ','  dLatCurrent, nvl(substr(el.dLon, 1,10), 'NaN') || ','  dLonCurrent,
   	nvl(substr(el.dDepth,1,10), 'NaN') || ','  dDepthCurrent, nvl(substr(el.tOrigin,1,13), 'NaN') || ','  tOriginCurrent,
   	nvl(substr(el.iUsedPh,1,5), 'NaN') || ','  iUsedPhCurrent,
   	nvl(substr(AOGlass.tinserted-p.OT,1,10), 'NaN') || ',' tFirstGlass,
   	nvl(substr(BHPInit.tOrigin,1,13), 'NaN') || ','  tOriginFirst, nvl(substr(round(BHPInit.dPrefMag,1),1,10), 'NaN') || ','   MagFirst,
   	nvl(substr(BHPInit.dLat,1,10), 'NaN') || ','  dLatFirst, nvl(substr(BHPInit.dLon,1,10), 'NaN') || ','  dLonFirst,
   	nvl(substr(BHPInit.dDepth,1,3), 'NaN') || ','  dDepthFirst,
   	nvl(substr(BHPAuto.tOrigin,1,13), 'NaN') || ','  tOriginAuto, nvl(substr(round(BHPAuto.dPrefMag,1),1,10), 'NaN') || ','   MagAuto,
   	nvl(substr(BHPAuto.dLat,1,10), 'NaN') || ','  dLatAuto, nvl(substr(BHPAuto.dLon,1,10), 'NaN') || ','  dLonAuto,
   	nvl(substr(BHPAuto.dDepth,1,3), 'NaN') || ','  dDepthAuto,
   	nvl(substr(BHP10.tOrigin,1,13), 'NaN') || ','  tOrigin10, nvl(substr(round(BHP10.dPrefMag,1),1,10), 'NaN') || ','   Mag10,
   	nvl(substr(BHP10.dLat,1,10), 'NaN') || ','  dLat10, nvl(substr(BHP10.dLon,1,10), 'NaN') || ','  dLon10,
   	nvl(substr(BHP10.dDepth,1,3), 'NaN') || ','  dDepth10,
   	ipdenum || ',' , nvl(felt,0) || ','  felt
 from PDEHydra p,
  	ALL_ACTUALORIGIN_INFO aoInitial,
      ALL_BHPELLETDESCS InitRel,
      ALL_EPOnly_EventPassport_INFO EPQR,
  	ALL_ACTUALORIGIN_INFO_WA aoExt,
  	ALL_ACTUALORIGIN_INFO aoATI,
  	ALL_ACTUALORIGIN_INFO aoATX,
  	ALL_ACTUALORIGIN_INFO aoPTI,
  	ALL_ACTUALORIGIN_INFO aoPTX,
      ALL_EVENTS_INFO_NO_NPH el,
      All_EventClaim_Info ecmin,
      All_EventClaim_Info ecmax,
      ALL_BHPELLETDESCS BHPResp,
      ALL_BHPELLETDESCS BHPMTOnly,
      ALL_EPOnly_EventPassport_INFO EPMTOnly,
      ALL_BHPELLETDESCS BHPAuto,
      ALL_BHPELLETDESCS BHP10,
      ALL_BHPELLETDESCS BHPInit,
  	ALL_ACTUALORIGIN_INFO aoHInit,
  	ALL_ACTUALORIGIN_INFO aoHQR,
  	ALL_ACTUALORIGIN_INFO aoHF,
  	ALL_ACTUALORIGIN_INFO aoH10,
  	ALL_ACTUALORIGIN_INFO aoHAuto,
  	ALL_ACTUALORIGIN_INFO aoGlass
 where p.idAOInitial = aoInitial.idActualOrigin(+)
   and p.idBHPQR = InitRel.idBHPellet(+)
   and p.idAOExt = AOExt.idActualOrigin(+)
   and p.idAOATI = aoATI.idActualOrigin(+)
   and p.idAOATX = aoATX.idActualOrigin(+)
   and p.idAOPTI = aoPTI.idActualOrigin(+)
   and p.idAOPTX = aoPTX.idActualOrigin(+)
   and p.idevent = el.idevent(+)
   and p.idECInit = ECMin.idEventClaim(+)
   and p.idECLast = ECMax.idEventClaim(+)
   and p.idBHPResp = BHPResp.idBHPellet(+)
   and p.idBHPMTOnly = BHPMTOnly.idBHPellet(+)
   and p.idEPMTOnly = EPMTOnly.idEventPassport(+)
   and p.idEPQR = EPQR.idEventPassport(+)
   and p.idBHPAuto = BHPAuto.idBHPellet(+)
   and p.idBHP10 = BHP10.idBHPellet(+)
   and p.idBHPInit = BHPInit.idBHPellet(+)
   and p.idAOHInit = AOHInit.idActualOrigin(+)
   and p.idAOHQR = AOHQR.idActualOrigin(+)
   and p.idAOHF = AOHF.idActualOrigin(+)
   and p.idAOH10 = AOH10.idActualOrigin(+)
   and p.idAOHAuto = AOHAuto.idActualOrigin(+)
   and p.idAOGlass = AOGlass.idActualOrigin(+)
'''

QUERY_COLUMNS = ['EVENTCODE','TDETECTLATENCY','TORIGINPDE','MAGPDE','DLATPDE','DLONPDE','DDEPTHPDE',
                 'IUSEDPHFINAL','TORIGININITIAL','MAGINITIAL','DLATINITIAL','DLONINITIAL','DDEPTHINITIAL',
                 'DERRMAXHKM','DERRMAXZKM','SCHEDULED_ANALYST','ANALYST1STCLAIM','T1STCLAIMED','ANALYSTLASTCLAIM',
                 'TLASTUNCLAIM','ANALYSTFIRSTPUB','QUICK','TFIRSTPUB','ANALYST2NDPUB','Q2UIC','TSECONDPUB',
                 'SINSTFIR','TFIRSTEXTP','TFIRSTATWC','TFIRSTPTWC','TFIRSTATWC','TFIRSTPTWC','INSHA',
                 'TBECAMERESP','EVENT_DATE_TIME','PREFMAGCURRENT','DLATCURRENT','DLONCURRENT','DDEPTHCURRENT',
                 'TORIGINCURRENT','IUSEDPHCURRENT','TFIRSTGLASS','TORIGINFIRST','MAGFIRST','DLATFIRST','DLONFIRST',
                 'DDEPTHFIRST','TORIGINAUTO','MAGAUTO','DLATAUTO','DLONAUTO','DDEPTHAUTO','TORIGIN10','MAG10',
                 'DLAT10','DLON10','DDEPTH10','iPDENum','Felt','iQCEventDataQuality','iQCFullPelletCount',
                 'iQCTimeShift']

def retrieveData(cursor,db,starttime,endtime):
    try:
        rc = cursor.var(cx_Oracle.NUMBER)
        res = cursor.callproc('qa_do_quarterly_prep',[rc,starttime,endtime])
    except cx_Oracle.DatabaseError, exc:
        error, = exc.args
        print "Oracle-Error-Code:", error.code
        print "Oracle-Error-Message:", error.message

    if rc.getvalue() != 0:
        print 'Report prep procedure failed: Error code %i' % rc

    nevents = getEventCount(cursor)
    #now do the monster query
    try:
        cursor.execute(MONSTER_QUERY)
    except cx_Oracle.DatabaseError, exc:
        error, = exc.args
        print "Oracle-Error-Code:", error.code
        print "Oracle-Error-Message:", error.message
    rows = cursor.fetchall()

    #sort the rows by origin time
    rows.sort(key = lambda row: (row[2]))
    
    return rows

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
        try:
            query = 'select get_start_of_pde(%i), get_end_of_pde(%i) from dual' % (pdenumber,pdenumber)
            cursor.execute(query)
            row = cursor.fetchone()
            pdemin = row[0]
            pdemax = row[1]
        except cx_Oracle.DatabaseError, exc:
            error, = exc.args
            print "Oracle-Error-Code:", error.code
            print "Oracle-Error-Message:", error.message
    return (pdemin,pdemax)

def getMostRecentPDE(cursor):
    query = 'select get_pde_from_ot(max(torigin)) from all_events_info_no_nph where iworkflowstatus = 8192'
    cursor.execute(query)
    pdestr = cursor.fetchone()[0]
    return pdestr

def getConnection(config):
    ip = config.get('DATABASE','ip')
    port = int(config.get('DATABASE','port'))
    user = config.get('DATABASE','user')
    password = config.get('DATABASE','password')
    sid = config.get('DATABASE','sid')
    tns = cx_Oracle.makedsn(ip,port,sid)
    db = cx_Oracle.connect(user,password,tns)
    cursor = db.cursor()
    return (db,cursor)

def getLastProcessed(datadir):
    qmatch = '\d{4}Q\d{1}.csv'
    wmatch = '\d{6}.csv'
    allfiles = os.listdir(datadir)
    weekfiles = []
    quarterfiles = []
    for afile in allfiles:
        if re.match(qmatch,afile) is not None:
            quarterfiles.append(afile)
        if re.match(wmatch,afile) is not None:
            weekfiles.append(afile)
    weekfiles.sort()
    quarterfiles.sort()
    if not len(weekfiles):
        lastweek = 190001
    else:
        lastweek,ext = os.path.splitext(weekfiles[-1])
        
    if not len(quarterfiles):
        lastquarter = '2013Q4'
    else:
        lastquarter,ext = os.path.splitext(quarterfiles[-1])
    
    return (int(lastweek),lastquarter)

def writeFile(rows,dfile):
    header = ','.join(QUERY_COLUMNS)
    f = open(dfile,'wt')
    f.write(header+'\n')
    for row in rows:
        rowstr = ''.join(row)
        f.write(rowstr+'\n')
    f.close()

def main(pdenumber):
    homedir = os.path.dirname(os.path.abspath(__file__)) #where is this script?
    configfile = os.path.join(homedir,'config.ini')
    config = ConfigParser.ConfigParser()
    config.readfp(open(configfile))
    datadir = config.get('OUTPUT','data')
    plotdir = config.get('OUTPUT','plots')
    lastweek,lastquarter = getLastProcessed(datadir)
    db,cursor = getConnection(config)
    if pdenumber is None:
        pdenumber = getMostRecentPDE(cursor)
    else:
        lastweek = 201352
    
    #weekly check
    if pdenumber > lastweek:
        starttime,endtime = getPDERange(cursor,pdenumber)
        startdate = datetime.utcfromtimestamp(starttime)
        enddate = datetime.utcfromtimestamp(endtime)
        rows = retrieveData(cursor,db,starttime,endtime)
        if not len(rows):
            print 'NO DATA FOUND FOR PDE #%i.  (%s to %s)' % (pdenumber,startdate,enddate)
            return
        weekfile = os.path.join(datadir,str(pdenumber)+'.csv')
        writeFile(rows,weekfile)

        #make weekly plots
        weekdir = os.path.join(plotdir,str(pdenumber))
        if not os.path.isdir(weekdir):
            os.mkdir(weekdir)
        neicq.makePlots(weekfile,weekdir)
        print 'Finished retrieving/plotting week number %i' % pdenumber

    #quarterly check
    qkeys = QUARTERS.keys()
    qkeys.sort()
    for key in qkeys:
        qstart,qend = QUARTERS[key]
        startpde = int(str(datetime.now().year) + '%02i' % qstart)
        endpde = int(str(datetime.now().year) + '%02i' % qend)
        qendstr = str(datetime.now().year)+key
        if qendstr <= lastquarter:
            break
        quarterstart,tmp = getPDERange(cursor,startpde)
        tmp,quarterend = getPDERange(cursor,endpde)
        if pdenumber < endpde:
            break
        rows = retrieveData(cursor,db,quarterstart,quarterend)
        quarter = str(datetime.now().year) + key
        quarterfile = os.path.join(datadir,quarter+'.csv')
        writeFile(rows,quarterfile)

        #make quarter plots
        qdir = os.path.join(plotdir,quarter)
        if not os.path.isdir(qdir):
            os.mkdir(qdir)
        neicq.makePlots(quarterfile,qdir)
        
        
    cursor.close()
    db.close()

if __name__ == '__main__':
    homedir = os.path.dirname(os.path.abspath(__file__)) #where is this script?
    if len(sys.argv) > 1:
        pdenumbers = [int(p) for p in sys.argv[1:]]
        for pdenumber in pdenumbers:
            main(pdenumber)
    else:
        main(None)
    sys.exit(0)
    
