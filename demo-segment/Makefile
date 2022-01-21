.PHONY: zip deploy-local invoke-local log
PROJECT_DIR=$(shell pwd)
SITE_PACKAGES_DIR=$(shell pipenv --venv)/lib/python3.9/site-packages
zip:
	@echo "Project Location: $(PROJECT_DIR)"
	@echo "Library Location: $(SITE_PACKAGES_DIR)"
	cd $(SITE_PACKAGES_DIR) && rm -rf __pycache__ && zip -r $(PROJECT_DIR)/bin/lambda.zip *
	cd $(PROJECT_DIR) && zip -g ./bin/lambda.zip lambda.py segment.py
	find ./bin/lambda.zip    

deploy:
	aws --endpoint-url=http://localhost:4566 \
    --region us-east-1 --profile localstack lambda delete-function \
    --function-name=segment
	
	aws lambda create-function \
    --function-name=segment \
    --runtime=python3.9 \
    --role=DummyRole \
    --handler=lambda.lambda_handler \
    --zip-file fileb://./bin/lambda.zip \
    --endpoint-url=http://localhost:4566

invoke:
	aws lambda --endpoint-url=http://localhost:4566 invoke \
	--function-name segment \
	--profile localstack  result.log

log:
	cat result.log