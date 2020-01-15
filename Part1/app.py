import re
import requests
from bs4 import BeautifulSoup
import csv
import os
import shutil
import logging
import time
import datetime
import boto
from boto.s3.key import Key
import sys


x = input("Enter CIK ")
cik = x.lstrip("0")

accNumber = input("Enter Accession Number ")

if len(cik) == 0 or len(accNumber) == 0:
    cik = "0000051143"
    accNumber = "0000051143-13-000007"

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

logName = "edgar_" + cik + "_" + '.txt'

logging.basicConfig(filename=logName, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

logging.debug("Log Initiated")
logging.debug("creating URL for the CIK provided")
accession = re.sub('[-]', '', accNumber);
print(accession)
url = 'https://www.sec.gov/Archives/edgar/data/' + cik + '/' + accession + '/' + accNumber + '-index.html'
print(url)


def checktag(param):

    logging.debug("Checking if the Paragraph tag has any indentation")
    set_flag = "false"
    datatable_tags = ["center", "bold"]
    for x in datatable_tags:
        if x in param:
            set_flag = "true"
    return set_flag


def createfolder(param):
    logging.debug("Creating folder for storing log files")
    title = param.find('filename').contents[0]
    if ".htm" in title:
        foldername = title.split(".htm")
        return foldername[0]


logging.debug("Trying to hit the generated URL")
request = requests.get(url)
if request.status_code == 200:
    logging.debug("Status = 200: Now creating link for 10-Q file")
    soup = BeautifulSoup(request.content, "html.parser")
    table = soup.find_all('table')[0]
    link = table.find('a')
    url_final = "https://www.sec.gov" + link.get('href')
    print(url_final)
    final_path = ''
    logging.debug("Trying to hit URL for 10-Q file")
    response = requests.get(url_final)
    soup1 = BeautifulSoup(response.text, 'html.parser')
    logging.debug("Finding all tables")
    all_tables = soup1.find_all('table')
    count = 0
    all_header = []
    for t in all_tables:
        logging.debug("Iterating through all tables")
        newTable = []
        logging.debug("Finding all rows in each table")
        trs = t.find_all('tr')
        for tr in trs:
            logging.debug("Checking style of row if it has color")
            if "background" in str(tr.get('style')) or "bgcolor" in str(tr.get('style')) or "background-color" in str(tr.get('style')):
                logging.debug("Background Style found: Finding parent table of the row")
                tr1 = tr.find_parent('table')
                gettable = []
                logging.debug("Iterating through all rows of the parent table")
                gettrs = tr1.find_all('tr')
                for x in gettrs:
                    row = []
                    colomn = []
                    gettd = x.find_all('td')
                    logging.debug("Iterating for coloumns of that row")
                    for y in gettd:
                        logging.debug("Fin  ")
                        r = y.text;
                        r = re.sub(r"['()]", "", str(r))
                        r = re.sub(r"[$]", " ", str(r))
                        if len(r) > 1:
                            r = re.sub(r"[—]", "", str(r))
                            colomn.append(r)
                    row = ([y.encode('utf-8') for y in colomn])
                    gettable.append(y.decode('utf-8').strip() for y in row)
                newTable = gettable
                break

            else:

                logging.debug("Nothing found row checking styling in Colomns")
                tds = tr.find_all('td')
                for td in tds:

                    if "background" in str(td.get('style')) or "bgcolor" in str(tr.get('style')) or "background-color" in str(tr.get('style')):
                        logging.debug("Background Style found: Finding parent table of the coloumn")
                        td1 = td.find_parent('table')
                        gettable = []
                        gettrs = td1.find_all('tr')
                        for x in gettrs:
                            row = []
                            colomn = []
                            gettd = x.find_all('td')
                            for y in gettd:
                                r = y.text
                                r = re.sub(r"['()]", "", str(r))
                                r = re.sub(r"[$]", " ", str(r))
                                if len(r) > 1:
                                    r = re.sub(r"[—]", "", str(r))
                                    colomn.append(r)
                            row = ([y.encode('utf-8') for y in colomn])
                            gettable.append(y.decode('utf-8').strip() for y in row)
                        newTable = gettable
                        break

            if not len(newTable) == 0:
                break

        if not len(newTable) == 0:
            count += 1
            ptag = t.find_previous('p')
            while ptag is not None and checktag(ptag.get('style')) == "false" and len(ptag.text) <= 1:
                logging.debug("Checking Paragraph tags styling for cleansing")
                ptag = ptag.find_previous('p')
                if checktag(ptag.get('style')) == "true" and len(ptag.text) >= 2:
                    global name
                    name = re.sub(r"[^A-Za-z0-9]+", "", ptag.text)
                    if name in all_header:
                        hrcount += 1
                        hrname = name + "_" + str(hrcount)
                        all_header.append(hrname)
                    else:
                        hrname = name
                        all_header.append(hrname)
                        break

            folder_name = createfolder(soup1)
            path = str(os.getcwd()) + "/" + folder_name
            if not os.path.exists(path):
                os.makedirs(path)
            if len(all_header) == 0:
                filename = folder_name + "-" + str(count)
            else:
                filename = all_header.pop()
            csv_name = filename + ".csv"
            csv_path = path + "/" + csv_name
            final_path = path;
            with open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
                logging.debug("Writing tables to CSV")
                writer = csv.writer(f)
                writer.writerows(newTable)

    logging.debug("Creating Zip file")
    shutil.make_archive("out", 'zip', final_path)



    bucketname = accesskey.lower() + "nuadsgroup3" + accNumber

    bucket = s3_connection.create_bucket(bucketname)
    logging.debug("Creating AWS S3 bucket " + bucketname)

    upload_to = Key(bucket)
    upload_to.key = 'problem1'

    logging.debug("Zip File & Log File Uploaded to AWS S3 bucket " + bucketname)
    upload_to.set_contents_from_filename(str("out" + ".zip"))
    upload_to.set_contents_from_filename(str(logName))
else:
    logging.debug("Error 404: CIK or Accession Number Does not exist!")
    print('Web site does not exist')


