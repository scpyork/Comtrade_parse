'''
Get all the csv files within a prefix and output them to read.files


read.files format:

	prefix columns
	prefix columns
	ets..

'''

import boto3,re

#Get the client
s3 = boto3.client('s3')

#A list of all csv files
files = s3.list_objects_v2(Bucket='trase-storage',Delimiter='/',Prefix='data/1-TRADE/STATISTICAL_DATA/GLOBAL/COMTRADE/')

tofile = open('read.files','w')

# for each file count the number of columns and append to file
for f in filter(lambda x: re.match(r'.*/COMTRADE_\d{4}\.csv',x),
                                [i['Key'] for i in files['Contents']]):

    obj = s3.get_object(Bucket='trase-storage', Key=f)
    body = obj['Body']
    line =  body._raw_stream.readline().split(',')
    tofile.write('%s %d\n'%(f,len(line)))
