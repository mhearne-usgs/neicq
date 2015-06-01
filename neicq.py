#!/usr/bin/env python

#stdlib imports
import csv
import sys
from datetime import datetime
from collections import Counter
import os.path

#third party imports
import matplotlib
matplotlib.use('Agg')
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import mstats
from matplotlib.dates import MonthLocator,DateFormatter

#local imports
from neicmap import distance
from neicutil import text

def filterMissingData(dataframe):
    nanfields = ['MAGPDE','DLATPDE','DLONPDE'] #fields which may have NaN values in them
    df = dataframe.copy()
    for field in nanfields:
        df = df[np.isfinite(df[field])]
    return df

def addTimeColumn(dataframe):
    #2014/06/10 16:01:27
    TIMEFMT = '%Y/%m/%d %H:%M:%S'
    etimes = []
    for i in range(0,len(dataframe)):
        dstr = dataframe['EVENT_DATE_TIME'][i]
        etimes.append(datetime.strptime(dstr,TIMEFMT))
    dataframe['etime'] = pd.Series(etimes)
    return dataframe

def createSeismicityMap(dataframe,plotdir):
    m = Basemap(projection='mill',lon_0=180)
    etimes = dataframe['etime']
    lat = dataframe['DLATPDE'].as_matrix()
    lon = dataframe['DLONPDE'].as_matrix()
    mag = dataframe['MAGPDE'].as_matrix()
    
    mintimestr = min(etimes).strftime('%b %d, %Y')
    maxtimestr = max(etimes).strftime('%b %d, %Y')
    lon[lon < 0] += 360 #longitudes less than 0 don't get plotted
    x,y = m(lon,lat) #convert lat/lon data into map coordinates

    i5 = (mag >= 5.0).nonzero()[0]
    i6 = (mag >= 6.0).nonzero()[0]
    i7 = (mag >= 7.0).nonzero()[0]
    i8 = (mag >= 8.0).nonzero()[0]
    
    #all events as green dots
    m.plot(x,y,'g.',markersize=3,zorder=5)
    #M5+ as blue circles with cyan face, size 3
    m.plot(x[i5],y[i5],'bo',markersize=3,markerfacecolor='#0080FF',zorder=6)
    #M6+ as blue circles with yellow face, size 5
    m.plot(x[i6],y[i6],'bo',markersize=5,markerfacecolor='#FFFF00',zorder=7)
    #M7+ as blue circles with red face, size 6
    m.plot(x[i7],y[i7],'bo',markersize=6,markerfacecolor='r',zorder=8)
    #M8+ as red pentagrams, size 16
    m.plot(x[i8],y[i8],'*',markersize=16,markerfacecolor='r',zorder=9)

    m.drawcoastlines(zorder=0)
    m.fillcontinents(color='#E1D8CF',lake_color='#81DAF5',zorder=1)
    m.drawparallels(np.arange(-90,90,30),labels=[1,0,0,0],zorder=2)
    m.drawmeridians(np.arange(m.lonmin,m.lonmax+30,60),labels=[0,0,0,1],zorder=3)
    # fill continents 'coral' (with zorder=0), color wet areas 'aqua'
    m.drawmapboundary(fill_color='#81DAF5',zorder=4)
    
    
    plt.title('%i events published from %s to %s' % (len(x),mintimestr,maxtimestr))
    plt.legend(['<5','5 to 5.9','6 to 6.9','7 to 7.9','>= 8'],numpoints=1,loc=3,fontsize=8)
    plt.savefig(os.path.join(plotdir,'seismicity.pdf'))
    plt.savefig(os.path.join(plotdir,'seismicity.png'))
    plt.close()
    print 'Creating seismicity.pdf'

def createDeltaPlots(dataframe,plotdir):
    df2 = dataframe.copy()
    df2 = df2[np.isfinite(df2['MAGINITIAL'])]
    firstmag = dataframe['MAGINITIAL'].as_matrix()
    lastmag = dataframe['MAGPDE'].as_matrix()
    firstdepth = dataframe['DDEPTHINITIAL'].as_matrix()
    lastdepth = dataframe['DDEPTHPDE'].as_matrix()
    firstlat = dataframe['DLATINITIAL'].as_matrix()
    lastlat = dataframe['DLATPDE'].as_matrix()
    firstlon = dataframe['DLONINITIAL'].as_matrix()
    lastlon = dataframe['DLONPDE'].as_matrix()

    dmag = lastmag - firstmag
    ddepth = lastdepth - firstdepth
    dloc = distance.sdist(firstlat,firstlon,lastlat,lastlon)/1000.0

    #get the number of each quantity
    nmag = len((np.abs(dmag) > 0.5).nonzero()[0])
    ndepth = len((np.abs(ddepth) > 50).nonzero()[0])
    ndist100 = len((np.abs(dloc) > 100).nonzero()[0])
    ndist50 = len((np.abs(dloc) > 50).nonzero()[0])
    
    #get the 90% quantiles for each difference
    ifinite = np.isfinite(dmag)
    qmag = mstats.mquantiles(np.abs(dmag[ifinite]),0.9)[0]
    qdepth = mstats.mquantiles(np.abs(ddepth[ifinite]),0.9)[0]
    qdist = mstats.mquantiles(np.abs(dloc[ifinite]),0.9)[0]
    
    ylabel = '# of earthquakes'
    
    fig,axeslist = plt.subplots(nrows=3,ncols=1)
    fig.set_size_inches(6,10)

    #mag change histogram
    plt.sca(axeslist[0])
    dmag[dmag > 1.0] = 1.0
    dmag[dmag < -1.0] = -1.0
    drange1 = np.arange(-2.05,2.05,0.1)
    drange2 = np.arange(-2.0,2.5,0.5)
    plt.hist(dmag,bins=drange1,align='mid')
    axlim = plt.axis()
    plt.xticks(drange2)
    #plt.axis([-1.1,1.1,axlim[2],axlim[3]])
    plt.ylabel(ylabel)
    plt.title('magnitude change (PDE-initial)')

    #depth change histogram
    ddepth[ddepth > 50] = 50
    ddepth[ddepth < -50] = -50
    plt.sca(axeslist[1])
    plt.hist(ddepth,bins=np.arange(-52.5,52.5,5),align='mid')
    axlim = plt.axis()
    plt.axis([-50.0,50.0,axlim[2],axlim[3]])
    plt.xticks(np.arange(-50,60,10))
    plt.ylabel(ylabel)
    plt.title('depth change [km] (PDE-initial)')

    #location change histogram
    dloc[dloc > 100] = 100
    plt.sca(axeslist[2])
    plt.hist(dloc,bins=np.arange(2.5,105.5,5),align='mid')
    plt.ylabel(ylabel)
    axlim = plt.axis()
    #plt.xticks(np.arange(0,100,20))
    plt.xticks(np.arange(0,120,20))
    plt.axis([0,105,axlim[2],axlim[3]])
    plt.title('epicentral change [km]')
    fig.tight_layout()
    plt.savefig(os.path.join(plotdir,'changes.pdf'))
    plt.savefig(os.path.join(plotdir,'changes.png'))
    plt.close()
    print 'Saving changes.pdf'
    return (nmag,ndepth,ndist100,ndist50,qmag,qdepth,qdist)

def createMagHist(dataframe,plotdir):
    lastmag = dataframe['MAGPDE'].as_matrix()
    plt.hist(lastmag,bins=np.arange(0.05,9.55,0.1),alpha=0.4)
    axlim = plt.axis()
    plt.axis([0,9.5,axlim[2],axlim[3]])
    plt.xticks(np.arange(0,10.0,0.5),rotation=-35)
    plt.ylabel('# of earthquakes')
    plt.xlabel('magnitude')
    plt.savefig(os.path.join(plotdir,'maghist.pdf'))
    plt.savefig(os.path.join(plotdir,'maghist.png'))
    plt.close()
    print 'Creating maghist.pdf'

def createSourceHist(dataframe,plotdir):
    source = dataframe['SINSTFIR'].tolist()
    c = Counter(source)
    s = pd.Series(c)
    s.sort(ascending=True)
    #s = s[s > 2]
    s.plot(kind='barh')
    plt.grid(False)
    fig = plt.gcf()
    fig.set_size_inches(8,10)
    ax = plt.gca()
    ax.tick_params(axis='both', labelsize=8)
    plt.ylabel('Source Code')
    plt.xlabel('number of hypocenters contributed')
    plt.savefig(os.path.join(plotdir,'sourcehist.pdf'))
    plt.savefig(os.path.join(plotdir,'sourcehist.png'))
    plt.close()
    print 'Creating sourcehist.pdf'

def createResponsePlot(dataframe,plotdir):
    mag = dataframe['MAGPDE'].as_matrix()
    response = (dataframe['TFIRSTPUB'].as_matrix())/60.0
    response[response > 60] = 60 #anything over 60 minutes capped at 6 minutes
    imag5 = (mag >= 5.0).nonzero()[0]
    imag55 = (mag >= 5.5).nonzero()[0]
    fig = plt.figure(figsize=(8,6))
    n,bins,patches = plt.hist(response[imag5],color='g',bins=60,range=(0,60))
    plt.hold(True)
    plt.hist(response[imag55],color='b',bins=60,range=(0,60))
    plt.xlabel('Response Time (min)')
    plt.ylabel('Number of earthquakes')
    plt.xticks(np.arange(0,65,5))
    ymax = text.ceilToNearest(max(n),10)
    yinc = ymax/10
    plt.yticks(np.arange(0,ymax+yinc,yinc))
    plt.grid(True,which='both')
    plt.hold(True)
    x = [20,20]
    y = [0,ymax]
    plt.plot(x,y,'r',linewidth=2,zorder=10)
    s1 = 'Magnitude 5.0, Events = %i' % (len(imag5))
    s2 = 'Magnitude 5.5, Events = %i' % (len(imag55))
    plt.text(35,.85*ymax,s1,color='g')
    plt.text(35,.75*ymax,s2,color='b')
    plt.savefig(os.path.join(plotdir,'response.pdf'))
    plt.savefig(os.path.join(plotdir,'response.png'))
    plt.close()
    print 'Saving response.pdf'

def createIncPlot(dataframe,plotdir):
    mag = dataframe['MAGPDE'].as_matrix()
    minmag = np.floor(mag.min())
    maxmag = np.ceil(mag.max())
    cdf = []
    mags = np.arange(minmag-0.05,maxmag+0.05,0.1)
    plt.figure(figsize=(6,6))
    for cmag in mags:
        cdf.append(np.sum(mag>=cmag))
    plt.semilogy(mags,cdf,'k+',markeredgecolor='k',markeredgewidth=1.5)
    plt.hold(True)
    [nn,xx] = np.histogram(mag,bins=mags)
    #xx in python defines the bin edges (length of nn +1), so take the mean of edges to get centers
    xx = (xx[0:-1] + xx[1:])/2.0
    plt.semilogy(xx,nn,'ro',markerfacecolor='none',linewidth=1.5,markeredgecolor='r',markeredgewidth=1.5)
    ii = np.argmax(nn)
    oldmags = mags+0.05
    estcomp = round(oldmags[ii]*10)/10
    plt.plot([estcomp,estcomp],[1,1e6])

    #plot lines
    m1mx = np.arange(1,8.5,0.5)
    aa = 7.5
    bb = 1
    plt.semilogy(m1mx,np.power(10,(aa-bb*m1mx)),'--',linewidth=1,color='b')
    aa = 7
    bb = 1
    plt.semilogy(m1mx,np.power(10,(aa-bb*m1mx)),'-.',linewidth=1,color='b')

    pxmax = max(8,maxmag+0.01)
    
    plt.axis([4,pxmax,np.power(10,0),np.power(10,4)])
    plt.xticks([4,5,6,7,8])
    axlim = plt.axis()
    plt.legend(['cumulative','incremental','est completeness %.1f' % estcomp,'b=1.0','b=1.0'],numpoints=1)
    plt.xlabel('magnitude',fontsize=18)
    plt.ylabel('number of earthquakes',fontsize=18)

    plt.savefig(os.path.join(plotdir,'incremental.pdf'))
    plt.savefig(os.path.join(plotdir,'incremental.png'))
    plt.close()
    print 'Saving incremental.pdf'

def createMagTimePlot(dataframe,plotdir):
    mag = dataframe['MAGPDE'].as_matrix()
    etime = dataframe['etime'].as_matrix()
    etimes = []
    for e in etime:
        ts = (e - np.datetime64('1970-01-01T00:00:00Z')) / np.timedelta64(1, 's')
        etimes.append(datetime.utcfromtimestamp(ts))
        
    plt.figure(figsize=(12,4))
    plt.plot(etimes,mag,'o',mfc='none',mec='b')
    months = MonthLocator(range(1, 13), bymonthday=1, interval=3)
    monthsFmt = DateFormatter("%b %d")
    ax = plt.gca()
    # ax.xaxis.set_major_locator(months)
    # ax.xaxis.set_major_formatter(monthsFmt)
    locs,labels = plt.xticks()
    plt.ylabel('magnitude')
    #plt.xlabel('month/day')

    plt.savefig(os.path.join(plotdir,'magtime.pdf'))
    plt.savefig(os.path.join(plotdir,'magtime.png'))
    plt.close()
    print 'Saving magtime.pdf'
    
def makePlots(datafile,plotdir):
    dataframe = pd.read_csv(datafile,index_col=False)
    dataframe = filterMissingData(dataframe)
    dataframe = addTimeColumn(dataframe)
    
    #remove events where final origin time - initial origin time > 100 seconds
    ntot = len(dataframe)
    t1 = dataframe['TBECAMERESP'].as_matrix().copy()
    t2 = dataframe['TORIGINPDE'].as_matrix().copy()
    t1[t1 == '<NEVER>'] = 9999999
    t1 = t1.astype(np.float)
    ivalid = np.where(t1 < (10*60))
    nvalid = len(ivalid[0])
    
    createSeismicityMap(dataframe,plotdir)
    nmag,ndepth,ndist100,ndist50,qmag,qdepth,qdist = createDeltaPlots(dataframe[ivalid],plotdir)
    createMagHist(dataframe,plotdir)
    createSourceHist(dataframe,plotdir)
    createResponsePlot(dataframe,plotdir)
    createIncPlot(dataframe,plotdir)
    createMagTimePlot(dataframe,plotdir)
    
    statsfile = os.path.join(plotdir,'statistics.txt')
    f = open(statsfile,'wt')
    f.write('TotalEvents: %i\n' % len(dataframe))
    f.write('TotalEvents: %i Response Events within 10 minutes\n' % nvalid)
    f.write('DeltaMag > 0.5: %i out of %i Percentage: %.2f%%\n' % (nmag,nvalid,(nmag/nvalid)*100))
    f.write('\t90%% of the magnitudes changed by %.1f or less.\n' % qmag)
    f.write('DeltaDepth > 50: %i out of %i Percentage: %.2f%%\n' % (ndepth,nvalid,(ndepth/nvalid)*100))
    f.write('\t90%% of the depths changed by %.1f km or less.\n' % qdepth)
    f.write('DeltaLoc > 100: %i out of %i Percentage: %.2f%%\n' % (ndist100,nvalid,(ndist100/nvalid)*100))
    f.write('DeltaLoc > 50: %i out of %i Percentage: %.2f%%\n' % (ndist50,nvalid,(ndist50/nvalid)*100))
    f.write('\t90%% of the epicenters changed by %.1f km or less.\n' % qdist)
    f.close()
    
if __name__ == '__main__':
    datafile = sys.argv[1]
    homedir = os.path.dirname(os.path.abspath(__file__)) #where is this script?
    makePlots(datafile,homedir)
    
