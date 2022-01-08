#!/bin/bash
aws lambda \
    --endpoint-url=http://localhost:4566 invoke \
    --function-name segment \
    --profile localstack result.log
