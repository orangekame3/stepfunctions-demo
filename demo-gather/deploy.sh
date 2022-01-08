#!/bin/bash
aws --endpoint-url=http://localhost:4566 \
    --region us-east-1 --profile localstack lambda delete-function \
    --function-name=gather

aws lambda create-function \
    --function-name=gather \
    --runtime=python3.9 \
    --role=DummyRole \
    --handler=lambda.lambda_handler \
    --zip-file fileb://lambda.zip \
    --endpoint-url=http://localhost:4566
