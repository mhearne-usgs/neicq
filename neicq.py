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
    if (max(etimes) - min(etimes)).days > 7:
        mintimestr = min(etimes).strftime('%b %Y')
        maxtimestr = max(etimes).strftime('%b %Y')
    else:
        mintimestr = min(etimes).strftime('%b %d, %Y')
        maxtimestr = max(etimes).strftime('%b %d, %Y')
    lon[lon < 0] += 360 #longitudes less than 0 don't get plotted
    try:
        x,y = m(lon,lat) #convert lat/lon data into map coordinates
    except Exception,msg:
        pass

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

    ylabel = '# of earthquakes'
    
    fig,axeslist = plt.subplots(nrows=3,ncols=1)
    plt.sca(axeslist[0])
    plt.hist(dmag,range=(-2.1,2.1),bins=30)
    plt.ylabel(ylabel)
    plt.title('magnitude change (PDE-initial)')
    plt.sca(axeslist[1])
    plt.hist(ddepth,range=(-100,100),bins=30)
    plt.ylabel(ylabel)
    plt.title('depth change [km] (PDE-initial)')
    plt.sca(axeslist[2])
    plt.hist(dloc,range=(0,200),bins=30)
    plt.ylabel(ylabel)
    plt.title('epicentral change [km]')
    fig.tight_layout()
    plt.savefig(os.path.join(plotdir,'changes.pdf'))
    plt.savefig(os.path.join(plotdir,'changes.png'))
    plt.close()
    print 'Saving changes.pdf'

def createMagHist(dataframe,plotdir):
    lastmag = dataframe['MAGPDE'].as_matrix()
    plt.hist(lastmag,bins=64)
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
    plt.text(35,85,s1,color='g')
    plt.text(35,75,s2,color='b')
    plt.savefig(os.path.join(plotdir,'response.pdf'))
    plt.savefig(os.path.join(plotdir,'response.png'))
    print 'Saving response.pdf'
    
    
def makePlots(datafile,plotdir):
    dataframe = pd.read_csv(datafile,index_col=False)
    dataframe = filterMissingData(dataframe)
    dataframe = addTimeColumn(dataframe)
    createSeismicityMap(dataframe,plotdir)
    createDeltaPlots(dataframe,plotdir)
    createMagHist(dataframe,plotdir)
    createSourceHist(dataframe,plotdir)
    createResponsePlot(dataframe,plotdir)
    
if __name__ == '__main__':
    datafile = sys.argv[1]
    homedir = os.path.dirname(os.path.abspath(__file__)) #where is this script?
    makePlots(datafile,homedir)
    
