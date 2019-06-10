'''
Read an allocated line from a job array task. 

This line corresponds to a location and the number of columns (number of separate processes)


'''



import sys,linecache
from pathos.multiprocessing import ProcessPool

args = linecache.getline('read.files',int(sys.argv[1])).split()

#psutil.virtual_memory()

print args






