# -*- coding: utf-8 -*-
"""
Created on Sun Apr 29 12:14:48 2018

Downloads the IVS microdata and pushes it into a postgresql database.

This is *much* slower than doing it in R with RPostgres::dbWrite(), but unlike
dbWrite, pandas' .to_sql method can write to a specified schema, which is what
I want.

@author: Peter
"""

import pandas as pd
from sqlalchemy import create_engine
import os
import shutil
import re
import urllib.request
import zipfile


# download and unzip source files
# os.chdir("D:/Peter/Documents/blog/ellisp.github.io/_working")

urllib.request.urlretrieve('http://www.mbie.govt.nz/info-services/sectors-industries/tourism/tourism-research-data/ivs/documents-image-library/vw_IVS.zip',
                           'ivs_tmp.zip')

with zipfile.ZipFile("ivs_tmp.zip","r") as zip_ref:
  zip_ref.extractall()


# Helper functions.  First one is modified from 
# https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-snake-case
def camel_to_snake(x):
  """Converts CamelCase to snake_case"""
  y = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', x)
  y = re.sub('([a-z0-9])([A-Z])', r'\1_\2', y).lower()
  return(y)

def csv_to_tabname(csvs):
  """Converts list of strings like 'vw_IVSTable - Name.csv' to 'table_name', 
  for writing to database
  """
  x = [re.sub("\.csv$", "", x) for x in csvs]
  x = [re.sub("^vw_IVS", "", x) for x in x]
  x = [re.sub(" - ", "", x) for x in x]
  x = [re.sub(" ", "_", x) for x in x]
  x = [camel_to_snake(i) for i in x]
  return(x)


# names of the files to import, limiting to those finishing with .csv
# in case in the future some metadata or readmes are added:
csvs = os.listdir("IVS")
csvs = [x for x in csvs if x.endswith('.csv')]

# names of the tables to write in the database
tabnames = csv_to_tabname(csvs)

# Connect to database. This uses peer authentication so no
# userid or password needed, it logs on as the user executing
# this Python program:
engine = create_engine('postgresql:///survey_microdata')

# import files and write to database, including some indexes
for i in range(len(csvs)):
  df = pd.read_csv('IVS/' + csvs[i], low_memory = False)
  df.columns = map(camel_to_snake, df.columns)
  df.to_sql(tabnames[i], engine, schema='nzivs',
            if_exists = 'replace',
            index = False)
  if 'survey_response_id' in df.columns :
    # index that column
    sql = "CREATE INDEX sri_" + tabnames[i] + " ON nzivs." + tabnames[i] + "(survey_response_id)"
    engine.execute(sql)

# cleanup, both the original downloaded zip file:
os.unlink('ivs_tmp.zip')        
# and the folder of unzipped CSVs:
shutil.rmtree('IVS')
