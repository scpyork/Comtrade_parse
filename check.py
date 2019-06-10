#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import re,sys,boto3
import codecs

def checkfile (fileKey ,delimiter = ',',header=0,stop = 1e999):
    '''
    check the file for incorrect numbers of columns
    skips any lines before header as metadata
    '''
    def reresub(x):
        return x.group().replace(',',';')

    with codecs.open(fileKey, "r", "utf-8") as f:
        incorrect = []
        for c,l in enumerate(f):
            if c>header:
                if c>stop:break

                if l.count(delimiter) != ncolumns:
                    l = re.sub(r'"[^"]*"', reresub,l)
                    if l.count(delimiter) != ncolumns:
                        print 'incorrect', c, l.count(delimiter) ,ncolumns, l
                        incorrect.append([c,l.count(delimiter)-ncolumns,ncolumns,l])
                    #else: print 'fix;',c


            elif c == header:
                ncolumns = l.count(delimiter)
                colnames = l.split(delimiter)



def checks3 (fileKey ,delimiter = ',',header=0,stop = 1e999 , verbose = True):
    '''
    check the s3file for incorrect numbers of columns
    skips any lines before header as metadata

    returns:
        [Line number,column difference,number columns,line content,Fixable?]

    '''
    def reresub(x):
        return x.group().replace(',',';')

    #if reading from list of files use an int input, else a string
    try: fileKey = tuple(open('read.files'))[int(fileKey)].split()[0]
    except:None

    #Get the client
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket='trase-storage', Key=fileKey)
    body = obj['Body']
    #line =  body._raw_stream.readline().split(',')


    incorrect = []
    for c,l in enumerate(body.iter_lines()):
            if c>header:
                if c>stop:break

                if l.count(delimiter) != ncolumns:
                    l = re.sub(r'"[^"]*"', reresub,l)
                    if l.count(delimiter) != ncolumns:
                        if verbose: print ('incorrect', c, l.count(delimiter) ,ncolumns, l)
                        incorrect.append([c,l.count(delimiter)-ncolumns,ncolumns,l,'Problem'])
                    else:

                        incorrect.append([c,l.count(delimiter)-ncolumns,ncolumns,l,'Fixable'])
                        if verbose: print ('fixable;',c)


            elif c == header:
                ncolumns = l.count(delimiter)
                colnames = l.split(delimiter)

    return incorrect,colnames
if __name__ == '__main__':
        q,c = checks3(4,verbose=False)
        problem = filter(lambda x : x[-1]=='Problem',q)
