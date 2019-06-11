import json,re,h5py
import numpy as np
import pandas as pd

'''
Input specify columns
Specify matches

matched = {
'column1': ['myfirstmatch', 'myothermatch'],
'column_numbers' : [3,4,55],
}

Must match combinations of all columns.

It is possible to adapt this to deal with inequalities.

'''
#what file to read
inputfile = 'combined_comtrade.hdf5'
#what to match
matched = {
'Commodity_Code': ['2308', '2309'],
}
#how much of the file to read in at once, lower this for low ram machines
chunk = 10000000



def get_data(f,matched,chunk=1e6):

    hf = h5py.File(f,mode='r')

    columns = list(hf)
    nrows = len(hf[columns[0]])

    print columns


    keep = []
    for start in xrange(0,nrows,chunk):
        end = start + chunk
        if end > nrows:
            end = nrows
        #print start, end , nrows
        indicies = False

        for c in matched.keys():
            for values in matched[c]:
                try:
                    matches += (hf[c][start:end]==values).astype(bool)
                except:
                    matches = (hf[c][start:end]==values).astype(bool)

            if not indices:
                indicies = set(np.arange(start,end)[matches])
            else:
                indices = set(np.arange(start,end)[matches])


        keep.extend(indicies)

    #save data in a dataframe
    #this works for easy writing to csv (small data)
    df = pd.DataFrame()

    for i in columns:
        df[i] = hf[i][keep]

    hf.close()
    return df


df = get_data(inputfile,matched,chunk)
df.to_csv('feed_comtrade.csv')
