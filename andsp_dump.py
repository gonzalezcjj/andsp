import sqlite3
import json
import codecs

conn = sqlite3.connect('content.sqlite')
cur = conn.cursor()

cur.execute('SELECT letitude,longitude,capitalcity FROM Country WHERE capitalcity <> ""')
fhand = codecs.open('andsp_places.js', 'w', "utf-8")
fhand.write("myData = [\n")
count = 0
for row in cur :
    data = row[0],row[1],row[2]
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
print(count, "records written to andsp_places.js")
print("Open andsp_places.html to view the data in a browser")
