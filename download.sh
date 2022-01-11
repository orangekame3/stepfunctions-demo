#!/bin/bash
aws s3 --endpoint-url=http://localhost:4566 cp s3://aggregatebucket/ ./result --exclude "*" --include "*.xlsx" --recursive
