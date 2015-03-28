"""
Import Verdigris energy data for H2O Sparkling Water Engine.

15_minute_energy.csv:
    Column 0: Time in seconds
    Columns 1-535: Electrical loads for circuits 0-534
"""

import predictionio
import argparse
import csv

def import_events(client, file):
    f = open(file, 'r')
    csvFile = csv.reader(f)

    print "Importing electric load data..."

    header = csvFile.next()
    csvData = {}
    for i in xrange(0, len(header)-1):
        csvData[i] = []

    for row in csvFile:
        # Iterate through each circuit. If data is present for the current
        # row, add it to csvData.
        for i in xrange(1,len(row)):
            if (row[i] != ''):
                timeAndEnergy = (row[0],row[i])
                csvData[i-1].append(timeAndEnergy)
    
    f.close()

    for circuitId in csvData:
        client.create_event(
            event = 'predict_energy',
            entity_type = 'electric_load',
            entity_id = circuitId,
            properties = {
                'circuitId': circuitId,
                'timeAndEnergy': csvData[circuitId]
            }
        )
    
    print "Imported data for %d circuits" % len(csvData)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
            description="Import energy load data for engine")
    parser.add_argument('--access_key', default='invald_access_key')
    parser.add_argument('--url', default="http://localhost:7070")
    parser.add_argument('--file', default="./data/15_minute_energy.csv")

    args = parser.parse_args()
    print args

    client = predictionio.EventClient(
        access_key=args.access_key,
        url=args.url,
        threads=5,
        qsize=500)
    import_events(client, args.file)
