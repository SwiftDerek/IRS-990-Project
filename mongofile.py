# -*- coding: utf-8 -*-
"""
Created on Fri Apr 26 14:08:16 2019

@author: Tom
"""
import pymongo
#import glom
#import pprint
import pandas as pd
from pandas.io.json import json_normalize
from pyspark import SparkContext
import numpy as np

#client = MongoClient('mongodb://54.89.111.43:27017/')
client = pymongo.MongoClient('54.89.111.43:27017', username='team29admin', password='pwonastickynote', \
                      authSource='admin', authMechanism='SCRAM-SHA-1')

db = client.irs990 

print(db)
col = db.returns

data = col.find({"return.ReturnHeader990x.schedule_parts.returnheader990x_part_i.RtrnHdr_TxYr":"2017",
                     "return.ReturnHeader990x.schedule_parts.returnheader990x_part_i.USAddrss_ZIPCd":{"$ne":None}}, 
{"return.ReturnHeader990x.schedule_parts.returnheader990x_part_i.BsnssNm_BsnssNmLn1Txt":1, 
    "return.ReturnHeader990x.schedule_parts.returnheader990x_part_i.USAddrss_ZIPCd":1,
    "return.IRS990.groups.Frm990PrtVIISctnA.PrsnNm":1,
    "return.IRS990.groups.Frm990PrtVIISctnA.RprtblCmpFrmOrgAmt":1,
   "return.IRS990.groups.CntrctrCmpnstn.BsnssNm_BsnssNmLn1Txt":1,
   "return.IRS990.groups.CntrctrCmpnstn.CntrctrCmpnstn_CmpnstnAmt":1  }).limit(100000)


fulldata = pd.DataFrame(columns= ['Name','Comp','Org', 'Zip', 'ReturnID'])
for ln in data:
    pdf = None
    pdf2 = None
    try:
        pdf = json_normalize(ln['return']['IRS990']['groups']['Frm990PrtVIISctnA'])
    except KeyError:
        pass
    try:
        pdf2 = json_normalize(ln['return']['IRS990']['groups']['CntrctrCmpnstn'])
    except KeyError:
        pass
    if pdf is not None and pdf2 is not None and len(pdf.columns) == 2 and len(pdf2.columns) == 2:
        pdf.columns = ['Name', 'Comp']
        pdf2.columns = ['Name', 'Comp']
        pdf = pdf.append(pdf2, ignore_index=True)
        pdf['Org'] = ln['return']['ReturnHeader990x']['schedule_parts']['returnheader990x_part_i']\
            ['BsnssNm_BsnssNmLn1Txt']
        pdf['Zip'] = ln['return']['ReturnHeader990x']['schedule_parts']['returnheader990x_part_i']\
            ['USAddrss_ZIPCd'][:5]
        pdf['ReturnID'] = ln['_id']
        fulldata = fulldata.append(pdf, ignore_index=True)
    elif pdf is not None and len(pdf.columns) == 2:
        pdf.columns = ['Name', 'Comp']
        pdf['Org'] = ln['return']['ReturnHeader990x']['schedule_parts']['returnheader990x_part_i']\
            ['BsnssNm_BsnssNmLn1Txt']
        pdf['Zip'] = ln['return']['ReturnHeader990x']['schedule_parts']['returnheader990x_part_i']\
            ['USAddrss_ZIPCd'][:5]
        pdf['ReturnID'] = ln['_id']
        fulldata = fulldata.append(pdf, ignore_index=True)
    elif pdf2 is not None and len(pdf2.columns) == 2:
        pdf2.columns = ['Name', 'Comp']
        pdf2['Org'] = ln['return']['ReturnHeader990x']['schedule_parts']['returnheader990x_part_i']\
            ['BsnssNm_BsnssNmLn1Txt']
        pdf2['Zip'] = ln['return']['ReturnHeader990x']['schedule_parts']['returnheader990x_part_i']\
            ['USAddrss_ZIPCd'][:5]
        pdf2['ReturnID'] = ln['_id']
        fulldata = fulldata.append(pdf2, ignore_index=True)
    else:
        pass

fulldata['Zip'] = fulldata['Zip'].astype(np.int64)
    
fulldata.to_csv('names_list.csv')



