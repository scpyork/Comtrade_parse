'''
A program to merge all h5files into one long one.
'''

import json,re,h5py,glob,sys
import numpy as np
import pandas as pd


files = glob.glob('data/COMTRADE*.hdf5')
files.sort()
columns = False
open_hf = [h5py.File(f,mode='r') for f in files]

columns = list(open_hf[0])
writehf = h5py.File('combined_comtrade.hdf5',mode='w')

tlength = sum([i[columns[0]].shape[0] for i in open_hf])
#dtypes = [(col,open_hf[0][col].dtype) for col in columns]




for i,col in enumerate(columns):
    #Create an empty dataset (column) to populate
    dset = writehf.create_dataset(col, compression="gzip",
      chunks=True, shape=(tlength,),
      maxshape=(None,),
      compression_opts=9,
      dtype=open_hf[0][col].dtype)
    counter = 0
    print (col,'\n')
    for hf in open_hf:
        sys.stdout.write('.')
        values = hf[col]
        dset[counter:counter+values.shape[0]] = values







#writehf.close()


'''
exists = False
for hf in open_hf:
    print hf
    for col in columns:
        if exists:
            writehf[col].resize((writehf[col].shape[0] + hf[col].shape[0]), axis = 0)
            writehf[col][-hf[col].shape[0]:] = hf[col]
        else:
            writehf.create_dataset(col,data = hf[col], compression="gzip", chunks=True, maxshape=(None,),
                                     compression_opts=9,dtype=hf[col].dtype)

    exists = True
    hf.close()

writehf.close()
'''
