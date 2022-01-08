#!/bin/bash
aws stepfunctions create-state-machine \
    --name Aggregate \
    --definition file://parallel.json \
    --role-arn "arn:aws:iam::000000000000:role/DummyRole" \
    --endpoint http://localhost:8083

aws stepfunctions start-execution \
    --state-machine arn:aws:states:us-east-1:123456789012:stateMachine:Aggregate \
    --endpoint http://localhost:8083
