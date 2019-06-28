import json
import csv

filePath = '/Users/user/Documents/test.json' # add your file path.
                                             # Keep in same folder

with open(filePath) as f:
    data = json.load(f)

file = 'output.csv'
fieldNames = ['fieldName1', 'fieldName2','fieldName3'] # column Names

with open(file, 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames = fieldNames, extrasaction='ignore')
    writer.writeheader()
    try:
        for i in range(len(data)):
            writer.writerow(data[i])
    except ValueError as e:
        print(f'I got a ValueError - reason {str(e)}')
        
print(f'Done. Wrote {len(data)} rows to {file}')
