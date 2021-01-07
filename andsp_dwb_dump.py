import sqlite3
import json
import codecs

conn = sqlite3.connect('content.sqlite')
cur = conn.cursor()

cur.execute('''SELECT d.year,
        	      d.population_value
                FROM Country   AS c,
                     Indicator AS i,
                     Data      AS d
                WHERE i.indicator_id = d.indicator_id
                AND   c.country_id = d.country_id
                ORDER BY d.year''')

fhand = codecs.open('andsp_dwb_sppopt.js', 'w', "utf-8")
fhand.write("sppopt = [ \n")
fhand.write("['Year','Population'],\n")
count = 0
for data_row in cur :
    val = str(data_row[0])
    data = val,data_row[1]
    try:
        js = json.dumps(data)
        count = count + 1
        if count > 1 : fhand.write(",\n")
        output = js
        fhand.write(output)
    except:
        continue

fhand.write("\n];\n")
cur.close()
fhand.close()
print(count, "Output written to andsp_dwb_sppopt.js")
print("Open andsp_dwb_view.htm to visualize the data in a browser")
