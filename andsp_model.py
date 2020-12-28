# Accessing New Data Sources Project (ANDSP)

import urllib.request, urllib.parse, urllib.error
from urllib.parse import urljoin
from urllib.parse import urlparse
import json
import sqlite3
import time
import ssl
import re
from datetime import datetime, timedelta

# The Data World Bank API. The following information will appear,
# when available, in the response when using this country query
# through the World Bank API:

# id: 3 letter ISO 3166-1 alpha-3 code
# iso2Code: 2 letter ISO 3166-1 alpha-2 code
# name: Name
# region: Name
# adminregion: Name
# incomeLevel: Name
# lendingType: Name
# capitalCity: Name of the country's capital city
# longitude: Geolocation
# latitude: Geolocation

# http://api.worldbank.org/v2/country/all?format=json

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

conn = sqlite3.connect('content.sqlite')
cur = conn.cursor()

cur.execute('''DROP TABLE IF EXISTS Country''');
conn.commit()

address = 'http://api.worldbank.org/v2/country/all?format=json'

cur.execute('''CREATE TABLE IF NOT EXISTS Country
            (id INTEGER UNIQUE PRIMARY KEY,
             third_id_isoletter TEXT,
             second_id_isoletter TEXT,
             name TEXT,
             region TEXT,
             adminregion TEXT,
             incomelevel TEXT,
             lendingtype TEXT,
             capitalcity TEXT,
             longitude REAL,
             letitude REAL)''')

try:
    # Open with a timeout of 30 seconds
    print('Retrieving from:', address)
    document = urllib.request.urlopen(address, None, 30, context=ctx)
    text = document.read().decode()
    if document.getcode() != 200 :
        print("Error code=",document.getcode(),address)

    try:
        # parse json object
        js = json.loads(text)
    except:
        js = None        

    if (len(js) > 0):
        print('... Data retrieved')
        conn.commit()

        for item in js[1]:
            # id: 3 letter ISO 3166-1 alpha-3 code
            dwb_id = item['id']
            # iso2Code: 2 letter ISO 3166-1 alpha-2 code
            dwb_iso2code = item['iso2Code']
            # name: Name
            dwb_name = item['name']
            # region: Name
            dwb_region = item['region']['value']
            # adminregion: Name
            dwb_adminregion = item['adminregion']['value']
            # incomeLevel: Name
            dwb_incomelevel = item['incomeLevel']['value']
            # lendingType: Name
            dwb_lendingtype = item['lendingType']['value']
            # capitalCity: name of the country's capital city
            dwb_capitalcity = item['capitalCity']
            # longitude: Geolocation
            dwb_longitude = item['longitude']
            # latitude: Geolocation
            dwb_latitude = item['latitude']

            cur.execute('''INSERT OR IGNORE INTO Country
                           (third_id_isoletter, second_id_isoletter, name, region, adminregion,
                            incomelevel, lendingtype, capitalcity, longitude, letitude)
                           VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )''', ( dwb_id, dwb_iso2code, dwb_name, dwb_region, dwb_adminregion, dwb_incomelevel, dwb_lendingtype, dwb_capitalcity, dwb_longitude, dwb_latitude))
            conn.commit()

    else:
        print('No data retrieved')

except KeyboardInterrupt:
    print('')
    print('Program interrupted by user...')
    #break
except Exception as e:
    print("Unable to retrieve or parse page",address)
    print("Error",e)

print('... Data stored')
cur.close()

