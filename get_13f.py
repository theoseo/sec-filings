import csv
import requests
import time
from bs4 import BeautifulSoup
from multiprocessing import Pool
from tqdm import *

#for year in range(2018, 2021):
#for qtr in range(1, 5):


year = 2020
qtr = 3

def get_report(data_row):
    cus_id = data_row['cus_id']
    data_url = data_row['data_url']

    url_13f = 'https://www.sec.gov/Archives/{}'.format(data_url)
    response_13f = requests.get(url_13f)

    if response_13f.status_code == requests.codes.ok:
        soup = BeautifulSoup(response_13f.text, 'lxml')

        tables = soup.find_all('infotable')

        parsed_data = open('./csv/{}_{}_{}.csv'.format(year, qtr, cus_id), 'w')
        csvwriter = csv.writer(parsed_data)        

        rows_head = []
        rows_head.append('year')
        rows_head.append('qtr')        
        rows_head.append('cus_id')                
        rows_head.append('nameofissuer')
        rows_head.append('titleofclass')
        rows_head.append('cusip')
        rows_head.append('value')
        rows_head.append('sshprnamt')
        rows_head.append('sshprnamttype')
        rows_head.append('investmentdiscretion')
        rows_head.append('othermanager')
        csvwriter.writerow(rows_head)
        
        for rows in tables:

            rowdata = []
            rowdata.append(data_row['year'])
            rowdata.append(data_row['qtr'])            
            rowdata.append(cus_id)                        
            rowdata.append(rows.find('nameofissuer').text)
            rowdata.append(rows.find('titleofclass').text)
            rowdata.append(rows.find('cusip').text)
            rowdata.append(rows.find('value').text)
            rowdata.append(rows.find('sshprnamt').text)
            rowdata.append(rows.find('sshprnamttype').text)
            rowdata.append(rows.find('investmentdiscretion').text)
            if rows.find('othermanager') != None:
                rowdata.append(rows.find('othermanager').text)
            csvwriter.writerow(rowdata)
            #for column in rows.children:
            #    print(column)

        parsed_data.close()   
        now = time.time()

        #print("Loop {}".format(now -loop_start))                 
        return 1
    else:
        return 0



url = 'https://www.sec.gov/Archives/edgar/full-index/{}/QTR{}/master.idx'.format(year, qtr)
response = requests.get(url)        
master_list = response.text.split('\n')

filing_list = []
for idx, line in enumerate(master_list[11:]):
    loop_start = time.time()
    #print(idx)
    items = line.split('|') 
    if len(items) > 2 and items[2] == '13F-HR':
        #print(idx, line)
        data = line.split('|')
        data_url = data[4]
        cus_id = data[0]
        #print('cus_id : {}, data : {}'.format(cus_id, data_url))
        
        filing_list.append({'year':year, 'qtr': qtr, 'cus_id' : cus_id, 'data_url' : data_url})

with Pool(processes=3) as p:
    max_ =  len(filing_list)
    with tqdm(total=max_) as pbar:
        for i, _ in enumerate(p.imap_unordered(get_report, filing_list)):
            pbar.update()
        '''
        '''

#print(filing_list)