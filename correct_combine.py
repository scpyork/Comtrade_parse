#!/usr/bin/env python
# coding: utf-8

import re,boto3
import time,h5py
import numpy as np


##  Functions  ##
#################

def typeset(v):
        '''convert into typearr (type array)'''
        x,y = v
        #print np.array(x)[0].astype(y),y
        z =  np.array(x)[0].astype(y)
        return z

def nulldata(x):
        '''remove empty cells'''
        if x!='': return x
        else: return -999 #np.nan#'-999'

#fn to nuliffy the whole matrix
vnull = np.vectorize(nulldata)

def reresub(x):
        ''' secondary substitution on
        matched regular expressions'''
        return x.group().replace(',',';')


######################
## main convert script
######################



def parse_s3 (fileKey, typearr = False, delimiter = ',', header=0,chunk = 1500000, location = './'):
    '''
    A remaker for the csv data once all inacuracies have been fixed.
    Rows not matching the header style will be ignored.

    Input Args:
        fileKey:   the file nam
        delimiter:  file separation
        header:     line number of column names.
        chunk:      after how many entries to save - the larger the better, although this may cause a problem for lower ram computers
        location:   where to save

        typearr: If provided we create a h5 file with the provided types.
    '''
    start_time = time.time()

    # If reading from list of files use an int input, else a string
    try: fileKey = tuple(open('read.files'))[int(fileKey)].split()[0]
    except:None # The filelocation is given already

    #Get the client
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket='trase-storage', Key=fileKey)

    # Get the number of lines
    nlines = 0
    for _ in obj['Body'].iter_lines(): nlines+=1

    obj = s3.get_object(Bucket='trase-storage', Key=fileKey)
    body = obj['Body']
    tempdata=[]
    #for each line in the open file
    for c,l in enumerate(body.iter_lines()):


        # If we are not in the header...
        if c>header:
                # To save memory resources, we process the file <chunk> lines at a time
                if c%chunk == 0 :
                    # write the lines to the csv file
                    csv.write('\n'.join([','.join(i) for i in tempdata])+'\n')

                    if typearr:
                        print (float(c)/float(nlines)*100.,'% done',(time.time() - start_time)/60., 'minutes')
                        tempdata = vnull(np.matrix(tempdata)).T # nonzero matrix
                        iterl = [[x,typearr[colnames[i]]] for i,x in enumerate(tempdata)]


                        #for j,i in enumerate(iterl): print i[1],colnames[j],i[0][:4]

                        box = list(pool.map(typeset,iterl))
                        #print box, np.array(box).shape, len(box)

                    # If we already have the h5 file
                    if c != chunk:
                        for i,cn in enumerate(colnames):
                            hf[cn].resize((hf[cn].shape[0] + box[i].shape[0]), axis = 0)
                            hf[cn][-box[i].shape[0]:] = box[i]

                    # If this is the first time create h5 datasets
                    else:
                        for i,cn in enumerate(colnames):
                            #print i,cn,typearr[cn], box[i]
                            dset = hf.create_dataset(cn,data = box[i], compression="gzip", chunks=True, maxshape=(None,),
                                                 compression_opts=9,dtype=typearr[cn])


                    #reset chunk array
                    tempdata = []



                if l.count(delimiter) != ncolumns:
                    l = re.sub(r'"[^"]*"', reresub,l)
                    if l.count(delimiter) != ncolumns:
                        print ('skipping line:', c,' with <sections> out of <header-sections>', l.count(delimiter) ,ncolumns, l)

                tempdata.append(l[:-1].split(delimiter))


            ###############################################
            ## get column data, and open transferable files
        elif c == header:
                csv = open(location + fileKey.split('/')[-1].split('.')[0]+'_corrected.csv','w')

                csv.write(l+'\n')
                colnames = re.sub('[^A-z0-9,]','',l.replace(' ','_')).split(delimiter)
                ncolumns = len(colnames)-1

                if typearr:
                    hf = h5py.File(location + fileKey.split('/')[-1].split('.')[0]+'.hdf5',mode='w')
                    # create pool for multiprocessing - use the number of columns if possible.
                    print ('Columns are reformatted into natural strings with no spaces. This is needed for easy hdf5 referencing.')
                    from multiprocessing import Pool
                    pool = Pool(processes=ncolumns)



                print ('Columns', colnames)



    # Update and incomplete chunks
    csv.write('\n'.join([','.join(i) for i in tempdata])+'\n')
    csv.close()
    if typearr:
        tempdata = vnull(np.matrix(tempdata)).T # nonzero matrix
        iterl = [[x,typearr[colnames[i]]] for i,x in enumerate(tempdata)]
        box = list(pool.map(typeset,iterl))#map(typeset,iterl)
        for i,cn in enumerate(colnames):
            hf[cn].resize((hf[cn].shape[0] + box[i].shape[0]), axis = 0)
            hf[cn][-box[i].shape[0]:] = box[i]

        hf.close()
        pool.close()

    return 1


if __name__=='__main__':
    '''args '''
    import json,sys
    location = './'
    typearr = json.load(open('typearrays.json'))
    parse_s3(int(sys.argv[1]),typearr=typearr)
