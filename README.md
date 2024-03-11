# Influx2 Results Processor

A processor of the Opsview Results Exporter using the JSON file format.

``` shell
usage: i2rp [-h] --url URL --token TOKEN --org ORG --bucket BUCKET --file FILE

Influx2 Results Processor: Process and import Opsview Results Exporter data into InfluxDB 2.x.

options:
  -h, --help       show this help message and exit
  --url URL        The URL of the InfluxDB instance.
  --token TOKEN    The token for authentication.
  --org ORG        The organization name.
  --bucket BUCKET  The bucket name where data will be stored.
  --file FILE      The path to the input file containing data entries.

```
