#!/bin/bash

RAW_FILE=https://raw.githubusercontent.com/nyphilarchive/PerformanceHistory/master/Programs/json/complete.json

OUT_FILE_NAME=raw_programs.json.gz
DATA_DIR=data

mkdir -p $DATA_DIR
curl $RAW_FILE | gzip > $DATA_DIR/$OUT_FILE_NAME
