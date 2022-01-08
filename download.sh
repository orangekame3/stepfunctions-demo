#!/bin/bash
aws s3 --endpoint-url=http://localhost:4566 cp s3://test-bucket/ ./folder --exclude "*" --include "*.xlsx" --recursive
