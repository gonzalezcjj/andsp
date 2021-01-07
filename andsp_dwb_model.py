# Accessing New Data Sources Project (ANDSP)

import urllib.request, urllib.parse, urllib.error
from urllib.parse import urljoin
from urllib.parse import urlparse
import json
import sqlite3
import ssl
import re

# The Data World Bank API. The following information will appear,
# when available, in the response when using this country query
# through the World Bank API:

# indicator id: Alphanumeric code of the indicator, SP.POP.TOTL
# indicator value: Name of the indicator, Population total
# country id: 2 letters ISO Code, VE
# country value: Country name, Venezuela RB
# countryiso3code: 3 letter ISO 3166-1 alpha-3 code, VEN
# date:	Year of the date
# value: Population number

# The URL for the API call. You can replace the ve parameter by the country isocode
# http://api.worldbank.org/v2/country/ve/indicator/SP.POP.TOTL?format=json

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

conn = sqlite3.connect('content.sqlite')
cur = conn.cursor()

cur.execute('''DROP TABLE IF EXISTS Indicator''');
cur.execute('''DROP TABLE IF EXISTS Country''');
cur.execute('''DROP TABLE IF EXISTS Data''');
conn.commit()

address = 'http://api.worldbank.org/v2/country/ve/indicator/SP.POP.TOTL?format=json'

cur.execute('''CREATE TABLE IF NOT EXISTS Indicator
               (indicator_id TEXT UNIQUE PRIMARY KEY,
                indicator_value TEXT)''')

cur.execute('''CREATE TABLE IF NOT EXISTS Country
               (country_id TEXT UNIQUE PRIMARY KEY,
                country_name TEXT,
                country_isothreeletter_code TEXT)''')

cur.execute('''CREATE TABLE IF NOT EXISTS Data
               (country_id TEXT,
                indicator_id TEXT,
                year INTEGER,
                population_value INTEGER)''')
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
            # indicator:  id:	"SP.POP.TOTL"
            dwb_indicator_id = item['indicator']['id']
            # indicator:  value:	"Population, total"
            dwb_indicator_value = item['indicator']['value']
            # country:   id:	"VE"
            dwb_country_id = item['country']['id']
            # country:   value:	"Venezuela, RB"
            dwb_country_name = item['country']['value']
            # countryiso3code:	"VEN"
            dwb_country_isothreeletter_code = item['countryiso3code']
            # date:	"YYYY"
            dwb_year = item['date']
            # value: "Number"
            dwb_population_value = item['value']

            cur.execute('''INSERT OR IGNORE INTO Indicator
                           (indicator_id, indicator_value)
                           VALUES ( ?, ? )''', ( dwb_indicator_id, dwb_indicator_value))

            cur.execute('''INSERT OR IGNORE INTO Country
                           (country_id, country_name, country_isothreeletter_code)
                           VALUES ( ?, ?, ? )''', ( dwb_country_id, dwb_country_name, dwb_country_isothreeletter_code))

            cur.execute('''INSERT OR IGNORE INTO Data
                           (country_id, indicator_id, year, population_value)
                           VALUES ( ?, ?, ?, ? )''', ( dwb_country_id, dwb_indicator_id, dwb_year, dwb_population_value))
            conn.commit()

    else:
        print('... No data retrieved')

    print('... Data stored')

except KeyboardInterrupt:
    print('')
    print('Program interrupted by user...')
    #break
except Exception as e:
    print("Unable to retrieve or parse page",address)
    print("Error",e)

cur.close()
