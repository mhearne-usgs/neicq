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

QUARTERS = {13:'Q1',26:'Q2',39:'Q3',52:'Q4'}

MONSTER_QUERY = '''select /*+ FIRST_ROWS(1) */
 nvl(substr(el.huidevent,1,8), 'NaN')  || ',' EventCode, nvl(substr(el.teventcreated - aoInitial.torigin, 1, 10), 'NaN')  || ',' tDetectLatency, 
       nvl(substr(p.OT,1,13), 'NaN') || ',' tOriginPDE, nvl(substr(round(p.Mag,1),1,10), 'NaN') || ','  MagPDE,
       nvl(substr(p.Lat,1,10), 'NaN') || ','  dLatPDE, nvl(substr(p.Lon,1,10), 'NaN') || ','  dLonPDE, 
	   nvl(substr(p.Depth,1,11), 'NaN') || ','  dDepthPDE, nvl(substr(NULL,1,10), 'NaN') || ','  iUsedPhPDE,
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
	   /* ,
       substr(el.sregion,1,30) region
	   */
 from PDEHydra p, 
      ALL_ACTUALORIGIN_INFO aoInitial, /* first origin for event in Hydra */
	  ALL_BHPELLETDESCS InitRel, /* params of first public release */
	  ALL_EPOnly_EventPassport_INFO EPQR, /* params of first public release */
      ALL_ACTUALORIGIN_INFO_WA aoExt, /* first origin for external agency */
      ALL_ACTUALORIGIN_INFO aoATI, /* first origin from ATWC - Internal */
      ALL_ACTUALORIGIN_INFO aoATX, /* first origin from ATWC - External */
      ALL_ACTUALORIGIN_INFO aoPTI, /* first origin from PTWC - Internal */
      ALL_ACTUALORIGIN_INFO aoPTX, /* first origin from PTWC - External */
	  ALL_EVENTS_INFO_NO_NPH el,
	  All_EventClaim_Info ecmin,
	  All_EventClaim_Info ecmax,
	  ALL_BHPELLETDESCS BHPResp,
	  ALL_BHPELLETDESCS BHPMTOnly, /* params of first public MT release */
	  ALL_EPOnly_EventPassport_INFO EPMTOnly, /* params of first public MT release */
	  ALL_BHPELLETDESCS BHPAuto,
	  ALL_BHPELLETDESCS BHP10,
	  ALL_BHPELLETDESCS BHPInit,
      ALL_ACTUALORIGIN_INFO aoHInit, /* Hydra origin from 1st bhpellet (approx) */
      ALL_ACTUALORIGIN_INFO aoHQR, /* Hydra origin from 1st QuickReview bhpellet (approx) */
      ALL_ACTUALORIGIN_INFO aoHF, /* Hydra origin from final pub/reviewed pellet (approx) */
      ALL_ACTUALORIGIN_INFO aoH10, /* Hydra origin from OT + 10 min Pellet */
      ALL_ACTUALORIGIN_INFO aoHAuto, /* Hydra origin from last auto pellet (before 1st PP entry) */
      ALL_ACTUALORIGIN_INFO aoGlass /* First Glass Origin */
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
;'''

QUERY_COLUMNS = ['EVENTCODE','TDETECTLATENCY','TORIGINPDE','MAGPDE','DLATPDE','DLONPDE','DDEPTHPDE',
                 'IUSEDPHFINAL','TORIGININITIAL','MAGINITIAL','DLATINITIAL','DLONINITIAL','DDEPTHINITIAL',
                 'DERRMAXHKM','DERRMAXZKM','SCHEDULED_ANALYST','ANALYST1STCLAIM','T1STCLAIMED','ANALYSTLASTCLAIM',
                 'TLASTUNCLAIM','ANALYSTFIRSTPUB','QUICK','TFIRSTPUB','ANALYST2NDPUB','Q2UIC','TSECONDPUB',
                 'SINSTFIR','TFIRSTEXTP','TFIRSTATWC','TFIRSTPTWC','TFIRSTATWC','TFIRSTPTWC','INSHA',
                 'TBECAMERESP','EVENT_DATE','_TIME','PREFMAGCURRENT','DLATCURRENT','DLONCURRENT','DDEPTHCURRENT',
                 'TORIGINCURRENT','IUSEDPHCURRENT','TFIRSTGLASS','TORIGINFIRST','MAGFIRST','DLATFIRST','DLONFIRST',
                 'DDEPTHFIRST','TORIGINAUTO','MAGAUTO','DLATAUTO','DLONAUTO','DDEPTHAUTO','TORIGIN10','MAG10',
                 'DLAT10','DLON10','DDEPTH10','iPDENum','Felt']

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
    cursor.execute(MONSTER_QUERY)
    rows = cursor.fetchall()

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

def getConnection(homedir):
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
    return (db,cursor)

def main():
    homedir = os.path.dirname(os.path.abspath(__file__)) #where is this script?
    lastfile = os.path.join(homedir,'lastprocessed.txt')
    if not os.path.isfile(lastfile):
        lastprocessed = 190001
    else:
        f = open(lastfile,'rt')
        lastprocessed = int(f.readline().strip())
        f.close()

    db,cursor = getConnection(homedir)
    pdenumber = getMostRecentPDE(cursor)

    #weekly check
    if pdenumber <= lastprocessed:
        print 'Most recent PDE (%i) is not newer than the most recently processed (%i).  Exiting.'
    starttime,endtime = getPDERange(cursor,pdenumber)
    startdate = datetime.utcfromtimestamp(starttime)
    enddate = datetime.utcfromtimestamp(endtime)

    rows = retrieveData(cursor,db,starttime,endtime)
    
    cursor.close()
    db.close()

if __name__ == '__main__':
    homedir = os.path.dirname(os.path.abspath(__file__)) #where is this script?
    main()
    sys.exit(0)
    
