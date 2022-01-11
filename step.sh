#!/bin/bash
aws stepfunctions create-state-machine \
    --name Aggregate \
    --definition file://parallel.json \
    --role-arn "arn:aws:iam::000000000000:role/DummyRole" \
    --endpoint http://localhost:8083
RESULT=$(aws stepfunctions start-execution \
    --state-machine arn:aws:states:us-east-1:123456789012:stateMachine:Aggregate \
    --endpoint http://localhost:8083)
sleep 30
ID=$(echo $RESULT | cut -c 78-113)

aws stepfunctions describe-execution \
    --execution-arn arn:aws:states:us-east-1:123456789012:execution:Aggregate:$ID \
    --endpoint=http://localhost:8083

aws stepfunctions delete-state-machine \
    --state-machine-arn "arn:aws:states:eu-east-1:123456789012:stateMachine:Aggregate" \
    --endpoint=http://localhost:8083
