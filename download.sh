#!/usr/bin/env bash

while read line; do
  if [[ "$line" =~ "M6C01" ]]; then
    # fname=$(awk 'END {print $1}' $line)
    fname=$(cut -f1 <<< $line)
    url="s3://noaa-goes17/ABI-L1b-RadF/2023/001/00/$fname"
    echo "Downloading $url"
    aws s3 cp --no-sign-request "$url" data/
  fi
done < files.txt
