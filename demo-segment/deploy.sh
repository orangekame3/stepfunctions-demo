#!/bin/bash
aws --endpoint-url=http://localhost:4566 \
    --region us-east-1 --profile localstack lambda delete-function \
    --function-name=segment

aws lambda create-function \
    --function-name=segment \
    --runtime=python3.9 \
    --role=DummyRole \
    --handler=lambda.lambda_handler \
    --zip-file fileb://lambda.zip \
    --endpoint-url=http://localhost:4566