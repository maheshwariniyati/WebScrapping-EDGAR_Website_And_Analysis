
# coding: utf-8

# In[1]:



# coding: utf-8

# In[6]:


import urllib.request
import zipfile
import os
import pandas as pd
import logging
import time
import datetime
import shutil
import glob
import sys
from itertools import groupby
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import boto
from boto.s3.key import Key

x = str(input("Enter Year: "))

#pass credentials
accesskey = input("Input AWS access key")
secretaccesskey = input("Input AWS secret access key")

#YOUR_ACCESS_KEY
aws_access_key_id = accesskey
#YOUR_SECRET_KEY
aws_secret_access_key = secretaccesskey

try:
    s3_connection = boto.connect_s3(aws_access_key_id, aws_secret_access_key)
    conn_check = s3_connection.get_all_buckets()

except:
    print("AWS keys invalid. Please try again")
    sys.exit()

logtime = time.time()
logdate = datetime.datetime.fromtimestamp(logtime).strftime('%Y%m%d_%H%M%S')

logName = "logs_" + x + '.txt'

logging.basicConfig(filename=logName, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

year=range(2003,2017)
if int(x) not in year:
    print("Data for the given year does not exist!")
    sys.exit(0)

matplotlib.use("Agg")
    
if not os.path.exists(x+'_zip'):
    os.makedirs(x+'_zip', mode=0o777)
    logging.info('Zipped file directory created!!')
else:
    shutil.rmtree(os.path.join(os.path.dirname("__file__"),x+'_zip'), ignore_errors=False)
    os.makedirs(x+'_zip', mode=0o777)
    logging.info('Zipped file directory created!!')
    
if not os.path.exists(x):
    os.makedirs(x, mode=0o777)
    logging.info('UnZipped file directory created!!')
else:
    shutil.rmtree(os.path.join(os.path.dirname("__file__"), x), ignore_errors=False)
    os.makedirs(x, mode=0o777)
    logging.info('UnZipped file directory created!!')

Quaters = {'Qtr1': ['01', '02', '03'], 'Qtr2': ['04', '05', '06'], 'Qtr3': ['07', '08', '09'], 'Qtr4': ['10', '11', '12']}
days= range(1, 32)
for key,value in Quaters.items():
    for val in value:
        for d in days:
            url= 'http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/' + str(x) + '/'+ str(key)+ '/log'+ str(x)+str(val)+str(format(d,'02d')) +'.zip'
            print(url)
            urllib.request.urlretrieve(url, x+'_zip/'+url[-15:])
            logging.info("Retrieving zipped log file")
            if os.path.getsize( x+'_zip/'+url[-15:]) <= 4515:
                os.remove( x+'_zip/'+url[-15:])
                logging.info("Log file is not present for "+ str(d) + " day")
                continue
            break

            
zip_files = os.listdir(x+'_zip')
for f in zip_files:
    z = zipfile.ZipFile(os.path.join(x+'_zip', f), 'r')
    for file in z.namelist():
        if file.endswith('.csv'):
            z.extract(file, x)
            logging.info(file +' successfully extracted to folder: unzippedfiles.')

allFiles = glob.glob(x+"/*csv")

if not os.path.exists('Reports'):
    os.makedirs('Reports', mode=0o777)
    logging.info('Graphical_images directory created!!')
else:
    shutil.rmtree(os.path.join(os.path.dirname("__file__"),'Reports'), ignore_errors=False)
    os.makedirs('Reports', mode=0o777)
    logging.info('Graphical_images directory created!!')

i=1
for file_ in allFiles:
    data = pd.read_csv(file_)
    logging.info("Calculating missing values in coloumns")
    print(data.isnull().sum())
    logging.info("Finding the NaN values in each coloumn")
    logging.info("Exploring Browser Coloumn")
    a = ['mie','fox','saf','chr','sea', 'opr','oth','win','mac','lin','iph','ipd','and','rim','iem']
    [len(list(group)) for key, group in groupby(a)] 
    logging.info("Grouping the values of all the browsers and storing in a dataframe")
    df = pd.DataFrame(data,columns = ['browser'])
    d = df.apply(pd.value_counts)
    logging.info("Counting the frequency of each browser type used in descending order")
    list(d.index)
    data['browser'].replace(np.nan,d.index[0], inplace = True) #Replacing the NaN values in the browser colomun with the max used browser
    data.isnull().sum()
    logging.info("confirming that no NaN values are present on the browser coloumn")
    logging.info("Working on the Size Coloumn")
    logging.info("Replacing the file size for ext : txt, by the mean of all the file size corresponding to txt")
    s = data[['extention','size']].groupby(data['extention'].str.contains('txt'))['size'].mean().reset_index(name='mean').sort_values(['mean'],ascending=False)
    data.loc[(data['size'].isnull()) & (data['extention'].str.contains('txt'))] = s
    data.reset_index(drop = True)
    logging.info("Replacing the file size with NaN values for ext : htm, by the mean of all the file size corresponding to htm") 
    g = data[['extention','size']].groupby(data['extention'].str.contains('htm'))['size'].mean().reset_index(name='mean').sort_values(['mean'],ascending=False)
    data.loc[(data['size'].isnull()) & (data['extention'].str.contains('htm'))] = g
    data.reset_index(drop=True)
    logging.info("Replacing the file size with NaN values for ext : xml, by the mean of all the file size corresponding to xml")
    h = data[['extention','size']].groupby(data['extention'].str.contains('xml'))['size'].mean().reset_index(name='mean').sort_values(['mean'],ascending=False)
    data.loc[(data['size'].isnull()) & (data['extention'].str.contains('xml'))] = h
    data.reset_index(drop=True)
    logging.info("To check how many NaN values are remaining ")
    logging.info("Replacing the file size for rest of the files with the mean of file size of txt extension, as it is the max used")
    data.loc[data['size'].isnull()] = s
    print(data.isnull().sum())
    logging.info("Working on all other coloumns")
    logging.info("If cik,Accession,ip,date are empty fields drop the records")
    data.dropna(subset=['cik'],inplace=True)
    data.dropna(subset=['accession'],inplace=True)
    data.dropna(subset=['ip'],inplace=True)
    data.dropna(subset=['date'],inplace=True)
    data.dropna(subset=['time'],inplace=True)
    logging.info("Calculating the max categorical value in other coloumns( code, zone,extention,idx,find) and filling the NaNs")
    data['code'].fillna(data['code'].max(),inplace=True)
    data['zone'].fillna(data['zone'].max(),inplace=True)
    data['extention'].fillna(data['extention'].max(),inplace=True)
    data['idx'].fillna(data['idx'].max(),inplace=True)
    data['find'].fillna(data['find'].max(),inplace=True)
    
    logging.info("Filling empty values with Categorical Values for coloumns (norefer,noagent,nocrawler)")
    data['norefer'].fillna(1,inplace=True)
    data['noagent'].fillna(1,inplace=True)
    data['crawler'].fillna(0,inplace=True)
    print(data.isnull().sum())
    logging.info("Missing data is handled successfully")          

#SUMMARY METRICS
    logging.info("Calculating Summary metrics of clean data")
    data.describe()
    data.reset_index(drop = True)
    logging.info("Mean and Median sizes for each Browser")
    brow_df = data.groupby('browser').agg({'size':['mean', 'median'],'crawler': len})
    brow_df.columns = ['_'.join(col) for col in brow_df.columns]
    data.reset_index(drop=True)
    print(brow_df)
                                                                                  
#To find out the 15 top searched CIKs 
    cik_df = pd.DataFrame(data, columns = ['cik'])
    d = cik_df.apply(pd.value_counts)
    logging.info("Top 15 most searched CIKs with the count")                                                                            
    d.head(15)
    data.reset_index(drop=True)
                    
#Compute distinct count of ip per month i.e. per log file
    ipcount_df = data['ip'].nunique()
    logging.info("Compute distinct count of ip per month i.e. per log file")
    print(ipcount_df)
                                                                                  
#Computing the count of status code on the basis of ip
    StCo_count=data[['code','ip']].groupby(['code'])['ip'].count().reset_index(name='count')
    logging.info("Computing the count of status code on the basis of ip")
    print(StCo_count)
    data.reset_index(drop=True)
                    
#Everything on per day basis
    #1. Average of Size 
    Avg_size=data[['date','size']].groupby(['date'])['size'].mean().reset_index(name='mean')
    logging.info("Average of file size is computed")
    print(Avg_size)
    #2. Number Of Requests
    Req_day=data[['date','ip']].groupby(['date'])['ip'].count().reset_index(name='count')
    logging.info("Number of request per day is computed")
    print(Req_day)
#Mean of file size on the basis of code status
    Mean_size=data[['code','size']].groupby(['code'])['size'].mean().reset_index(name='mean')
    logging.info("Mean of file size on the basis of code status")
    print(Mean_size)

    logging.info("Summary metrics computed succesfully!!")
#graph of no of status codes by browser
    try:
        logging.info("graphical analysis started")
        Num_of_codes=data[['browser','code']].groupby(['browser'])['code'].count().reset_index(name = 'count_code').sort_values(['count_code'],ascending=False)
        data.reset_index(drop = True)
        print(Num_of_codes)
        u= np.array(range(len(Num_of_codes)))
        y= Num_of_codes['count_code']
        xticks1 = Num_of_codes['browser']
        plt.xticks(u,xticks1)
        plt.bar(u,y)
        plt.title('Count of status code for all the browsers')
        plt.ylabel('Count of codes')
        plt.xlabel('Browsers')
        plt.savefig('Reports/countsperbrowser'+ str(i) +'.png',dpi=100)
        plt.clf()
        logging.info("graphical analysis end")
    except Exception as e:
        print(u)
        logging.error(str(e))
        logging.error("Error plotting the graph ")
    
#graph for max cik(10) by IP used
    try:
        logging.info("graphical analysis started")
        Num_of_CIKs=data[['cik','ip']].groupby(['cik'])['ip'].count().reset_index(name='count').sort_values(['count'],ascending=False).head(10)
        data.reset_index(drop=True)
        print(Num_of_CIKs)
        u = np.array(range(len(Num_of_CIKs)))
        y = Num_of_CIKs['count']
        xticks2 = Num_of_CIKs['cik']
        plt.xticks(u, xticks2)
        plt.bar(u,y)
        plt.title('Top 10 CIKs by IPs')
        plt.ylabel('Count of IPs')
        plt.xlabel('CIK-')
        plt.savefig('Reports/CIKsbyIPcount'+ str(i) +'.png',dpi=100)
        plt.clf()
        logging.info("graphical analysis end")
    except Exception as e:
        print(u)
        logging.error(str(e))
        logging.error("Error plotting the graph ")

#Graph of Mean of file size on the basis of code status

    try:
        Mean_size=data[['code','size']].groupby(['code'])['size'].mean().reset_index(name='mean').sort_values(['mean'],ascending=False)
        data.reset_index(drop=True)
        print(Mean_size)
        u = np.array(range(len(Mean_size)))
        y = Mean_size['mean']
        xticks3 = Mean_size['code']
        plt.xticks(u, xticks3)
        plt.bar(u,y)
        plt.title('filesize by codes')
        plt.ylabel('mean size')
        plt.xlabel('Code')
        plt.savefig('Reports/MeanSizeByCode'+str(i) +'.png',dpi=100)
        plt.clf()
    except Exception as e:
        print(u)
        logging.error(str(e))
        logging.error("Error plotting the graph ")
#Graph for average file size by extension
    try:
        Avg_size=data[['extention','size']].groupby(['extention'])['size'].mean().reset_index(name='mean').sort_values(['mean'],ascending=False).head(20)
        data.reset_index(drop=True)
        print(Avg_size)
        u = np.array(range(len(Avg_size)))
        y = Avg_size['mean']
        xticks4 = Avg_size['extention']
        plt.xticks(u, xticks4)
        plt.bar(u,y)
        plt.title('Avg File size by extention')
        plt.ylabel('MeanFileSize')
        plt.xlabel('Extention')
        #plt.savefig(os.path.join('Graphical_images',str(val),'filesizebyextention.png'),dpi=100)
        plt.savefig('Reports/filesizebyextention'+str(i) +'.png',dpi=100)
        #plt.show()
        plt.clf()
    except Exception as e:
        print(u)
        logging.error(str(e))
        logging.error("Error plotting the graph ")
#ANAMOLIES IN FILESIZE
    try:
        logging.info("Anomalies analysis started")
        data.boxplot(column='size',vert=True,sym='',whis=10,showfliers=False)
        plt.xticks(rotation=70)
        plt.title('Anomalies displayed on the file size')
        plt.ylabel('size')
        plt.savefig('Reports/'+'Anomalies'+ str(i) +'.png',dpi=100)
        #plt.show()
        logging.info("Anomalies analysis ended")
    except Exception as e:
        print(u)
        logging.error(str(e))
        logging.error("Error plotting the graph ")

    i = i+1

  
#Making a zip file having the log file and the Graphical_images folder
def make_zipfile(output_filename, source_dir):
    relroot = os.path.abspath(os.path.join(source_dir, os.pardir))
    with zipfile.ZipFile(output_filename, "w", zipfile.ZIP_DEFLATED) as zip:
        for root, dirs, files in os.walk(source_dir):
            # add directory (needed for empty dirs)
            zip.write(root, os.path.relpath(root, relroot))
            for file in files:
                filename = os.path.join(root, file)
                if os.path.isfile(filename):
                    arcname = os.path.join(os.path.relpath(root, relroot), file)
                    zip.write(filename, arcname)

make_zipfile('ADSAssign1Part2.zip','Reports')
print("Done")

bucketname = accesskey.lower() + "nuadsgroup3" + str(x)

bucket = s3_connection.create_bucket(bucketname)
logging.debug("Creating AWS S3 bucket " + bucketname)

upload_to = Key(bucket)
upload_to.key = 'problem2'

logging.debug("Zip File & Log File Uploaded to AWS S3 bucket " + bucketname)
upload_to.set_contents_from_filename(str("ADSAssign1Part2.zip"))
upload_to.set_contents_from_filename(str(logName))





