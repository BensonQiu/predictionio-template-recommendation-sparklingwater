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

    # Skip header line
    csvFile.next()

    for idx, row in enumerate(csvFile):
        print "Importing data from row ",idx
        for i in xrange(1,len(row)):
            if (row[i] != ''):
                # Circuits are zero-indexed
                circuitId = i-1
                time = row[0]
                energy = row[i]

                client.create_event(
                    event = 'predict_energy',
                    entity_type = 'electrical_load',
                    entity_id = circuitId,
                    properties = {
                        'circuitId': circuitId,
                        'time': time,
                        'energy': energy
                        }
                    )
    
    f.close()

    print "Done importing data."

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
