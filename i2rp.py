#!/usr/bin/env python3
import json
import argparse
import sys
from influxdb_client import InfluxDBClient, Point, WritePrecision, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS

def process_perf_data(point, entry):
    perf_data = entry["message"]["perf_data"]
    for key, value in perf_data.items():
        # Rename the "time" field if it conflicts with InfluxDB's reserved keyword
        if key == 'time':
            # Attempt to find a more suitable label for the "time" data
            suitable_labels = ['execution_time', 'response_time', 'poll_time']
            for label in suitable_labels:
                if label in entry["message"]:
                    key = label  # Rename key to the found label
                    break
            else:
                key = 'duration'  # Default rename if no suitable label is found
        try:
            if 'ms' in value:
                value = float(value.replace('ms', ''))
            elif 's' in value:
                value = float(value.replace('s', ''))
            elif 'bps' in value:
                value = int(value.replace('bps', ''))
            elif '%' in value:
                value = float(value.replace('%', ''))
            elif value.endswith('c'):
                value = float(value.replace('c', ''))
            else:
                value = float(value)
        except ValueError:
            pass
        point.field(key, value)
    return point

def process_entry(entry):
    point = Point("opsview_metrics")
    point.tag("hostname", entry["message"]["hostname"])
    if entry["message"].get("servicecheckname"):
        point.tag("servicecheckname", entry["message"]["servicecheckname"])
    if entry["message"].get("metadata"):
        for key, value in entry["message"]["metadata"].items():
            point.tag(key, value)
    point = process_perf_data(point, entry)
    point.time(entry["info"].split()[1] + "T" + entry["info"].split()[2] + "Z", WritePrecision.NS)

    return point

def main(args):
    if args.url.startswith("http://") or args.url.startswith("https://"):
        url_with_prefix = args.url
    else:
        url_with_prefix = "http://" + args.url

    client = InfluxDBClient(url=url_with_prefix, token=args.token, org=args.org)
    write_api = client.write_api(write_options=WriteOptions(batch_size=1000, flush_interval=30_000))

    with open(args.file_path, 'r') as file:
        entries = file.readlines()

    batch = []
    counter = 0
    total_entries = len(entries)

    for line in entries:
        try:
            entry = json.loads(line)
            point = process_entry(entry)
            batch.append(point)
            counter += 1

            sys.stdout.write(f"\rProcessed {counter} / {total_entries} successfully. Progress: {(counter / total_entries) * 100:.2f}%")
            sys.stdout.flush()

            if len(batch) >= 1000:
                write_api.write(bucket=args.bucket, org=args.org, record=batch)
                batch = []

        except Exception as e:
            print(f"\nFailed to write batch with error: {e}")
            sys.exit(1)

    if batch:
        try:
            write_api.write(bucket=args.bucket, org=args.org, record=batch)
        except Exception as e:
            print(f"\nFailed to write batch with error: {e}")
            sys.exit(1)

    print("\nAll data imported successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process and import data into InfluxDB 2.x.')
    parser.add_argument('--url', required=True, help='The URL of the InfluxDB instance.')
    parser.add_argument('--token', required=True, help='The token for authentication.')
    parser.add_argument('--org', required=True, help='The organization name.')
    parser.add_argument('--bucket', required=True, help='The bucket name where data will be stored.')
    parser.add_argument('--file_path', required=True, help='The path to the input file containing data entries.')

    args = parser.parse_args()
    main(args)
