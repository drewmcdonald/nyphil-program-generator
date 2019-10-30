#!/bin/bash

mkdir -p $DATA_DIR
curl $REMOTE_RAW_DATA_FILE | gzip > $LOCAL_RAW_DATA_FILE
