"""
Import Verdigris energy data for H2O Sparkling Water Engine.

The csv file is missing some data, so only import data from rows 1506-2984 for now. Also, for testing purposes, only import the time column and four columns of electrical load data.

15_minute_energy.csv:
    Column A(0): Time in seconds
    Column B(1): Plug Loads:Conference
    Column C(2): Plug Loads:Open Office
    Column D(3): Misc Equipment:Elevator
    Column E(4): Misc Equipment:Elevator
"""

import predictionio
import argparse
import csv

START_ROW = 1506
END_ROW = 2984

def import_events(client, file):
    f = open(file, 'r')
    csvFile = csv.reader(f)

    # Skip header line
    csvFile.next()
    print "Importing electric load data..."

    currRow = 0
    plugload_id = 0
    for row in csvFile:
        currRow += 1
        if (currRow < START_ROW):
            continue
        elif (currRow > END_ROW):
            break

        client.create_event(
            event = 'predict_energy',
            entity_type = 'electric_load',
            entity_id = str(currRow),
            properties = {
                'time': int(row[0]),
                'conference_load': float(row[1]),
                'openoffice_load': 0.0 if (row[2] == '') else float(row[2]),
                'elevator1_load': float(row[3]),
                'elevator2_load': float(row[4])
            }
        )
        
    f.close()
    print "%d rows are imported." % (END_ROW - START_ROW + 1)

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
