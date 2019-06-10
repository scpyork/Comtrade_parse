
import re,boto3
import time,h5py
import numpy as np


def find_s3_types(fileKey, delimiter = ',', header=0,nsamples = 30 ):
    '''
    testing for the column filetypes

    Input Args:
        filename:   the file name
        delimiter:  file separation
        header:     line number of column names.
    '''

    # If reading from list of files use an int input, else a string
    try: fileKey = tuple(open('read.files'))[int(fileKey)].split()[0]
    except:None # The filelocation is given already

    print (fileKey)

    #Get the client
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket='trase-storage', Key=fileKey)

    # Get the number of lines
    nlines = 0
    for _ in obj['Body'].iter_lines(): nlines+=1
    sample = (nlines-header)//nsamples
    print ('Number of lines: %d, sampling every %d'%(nlines,sample))
    obj = s3.get_object(Bucket='trase-storage', Key=fileKey)
    body = obj['Body']

    tempdata=[]

    print('begining scan')
    for c,l in enumerate(body.iter_lines()):
            if c>header:
                if c%sample == 0 and l.count(delimiter) == ncolumns:
                    tempdata.append(l[:-1].split(delimiter))
                    #print(c/float(nlines))
            elif c == header:
                print l
                ncolumns = l.count(delimiter)
                colnames = re.sub(r'[^A-z0-9,]','',l.replace(' ','_')).split(delimiter)
                print(colnames)


    #df = pd.DataFrame(tempdata,columns = colnames)
    print 'computing sample'
    tempdata = np.array(tempdata,order='F')


    print 'convert sample to type'
    typearr={}
    mytypes = ['S100','i4','f8']
    #preset = ['Qty', 'Netweight_kg', 'Trade_Value_US','Commodity_Code']

    isnumeric = re.compile(r'[\d\.]+')
    for i,j in enumerate(colnames):#- set(preset):
        dtype=1
        #print i, tempdata.shape,j
        for r in tempdata[:,i]:
                if r=='':pass
                elif isnumeric.match(str(r)):
                    if r.isdigit():dtype*=1
                    else: dtype=2
                else: dtype = 0 #kill run its a string
        typearr[j] = mytypes[dtype]

    #manual override
    typearr['Commodity_Code'] = 'S4'
    typearr['Classification'] = 'S4'
    #need more digits
    for i in ['Netweight_kg', 'Trade_Value_US']:typearr[i] = 'int64'

    return typearr






if __name__ == '__main__':

    '''
    From all files, get the typearrays and join
    '''
    from multiprocessing import Pool
    pool = Pool(processes=10)              # start 4 worker processes
    typearrs = pool.map(find_s3_types, range(len(tuple(open('read.files')))))          # prints "[0, 1, 4,..., 81]"
    print ('typearr done')
    finaltype = {}

    keys = set()
    for i in typearrs:
        keys = keys | set(i.keys())

    for k in keys:
        t = set([i[k] for i in typearrs])

        if len(t)==1:
            finaltype[k]=list(t)[0]
        else:
            print (t,k)
            final = max([j.lower() for j in t])
            if 's' in final: final = final.upper()
            print final
            finaltype[k] = final
    import json
    with open('typearrays.json', 'w') as outfile:
        json.dump(finaltype, outfile,indent=4, sort_keys=True)
