import json,re,h5py,glob
import numpy as np
import pandas as pd

hscodes = json.load(open('HS.json'))

#what we care / want to ignore, separated by pipes
ignore = re.compile(r'paper|cat|dogs',re.IGNORECASE)
keyword = re.compile(r'feed|feeding',re.IGNORECASE)

contains = []
for prefix in hscodes:
    prefix_data = hscodes[prefix]
    for number in prefix_data:
        description = prefix_data[number].lower()
        if keyword.search(description) and not ignore.search(description):
            contains.append([prefix,number,prefix_data[number]])


contains = np.array(contains)
#for i in contains:print i

classification = set(contains[:,0])
codes = set(contains[:,1])


files = glob.glob('*.hdf5')


def get_data(f):

    hf = h5py.File(f,mode='r')



    columns = list(hf)
    nrows = len(hf[columns[0]])
    chunk = 10000000

    keep = []
    for start in xrange(0,nrows,chunk):
        end = start + chunk
        if end > nrows:
            end = nrows
        #print start, end , nrows

        keep.extend(
        np.arange(start,end)
        [(hf['Commodity_Code'][start:end]=='2308').astype(bool)+
         (hf['Commodity_Code'][start:end]=='2309').astype(bool)
        ]
        )

    df = pd.DataFrame()

    for i in columns:
        df[i] = hf[i][keep]

    print f
    return df
from multiprocessing import Pool
pool = Pool(processes=16)
rtn =  pool.map(get_data,files)


all = pd.concat(rtn)
all.to_csv('feed_comtrade.csv')
