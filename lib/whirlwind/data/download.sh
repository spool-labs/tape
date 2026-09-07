#!/bin/bash
# Downloads the WonderProxy public ping dataset (2020-07-19..20): real inter-city
# latency measurements between 200+ servers worldwide, used for the green network
# layer of the Whirlwind latency histogram.
#
# Source: https://wonderproxy.com/blog/a-day-in-the-life-of-the-internet/
# The bucket was renamed from wp-public to wp-public-data.
set -euo pipefail
cd "$(dirname "$0")"
if [ ! -f pings.csv ]; then
    echo "Downloading ping dataset (~51 MiB gzip, ~208 MiB raw)..."
    curl -sSL -o pings.csv.gz "https://wp-public-data.s3.amazonaws.com/pings/pings-2020-07-19-2020-07-20.csv.gz"
    gzip -d pings.csv.gz
else
    echo "pings.csv already present"
fi
