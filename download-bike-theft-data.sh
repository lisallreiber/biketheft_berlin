#!/bin/bash

# parameterized script to download the data each day
# and append it to the existing file

# get the date
if [ -z "$1" ]; then
    # Set default value to today's date
    TODAY=$(date +%Y-%m-%d)
else
    TODAY=$1
fi

URL="https://www.internetwache-polizei-berlin.de/vdb/Fahrraddiebstahl.csv"

LOCAL_PREFIX="data/raw"
FILE_NAME="${TODAY}_berlin-bike-theft.csv"
LOCAL_PATH="${LOCAL_PREFIX}/${FILE_NAME}"

# download the data
echo "
... downloading ${URL} 
... for data up to $TODAY 
... to location: ${LOCAL_PATH}
"
# generate folder data/raw if it doesn't exist
mkdir -p "${LOCAL_PREFIX}"
wget ${URL} -O ${LOCAL_PATH}

# echo the number of lines
wc -l "${LOCAL_PATH}"
