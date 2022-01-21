.PHONY: zip deploy-local invoke-local log download stepfunction
zip:
	cd demo-scatter && make -f Makefile zip --no-print-directory
	cd demo-segment && make -f Makefile zip --no-print-directory
	cd demo-gather && make -f Makefile zip --no-print-directory

deploy-local:
	cd demo-scatter && make -f Makefile deploy-local --no-print-directory
	cd demo-segment && make -f Makefile deploy-local --no-print-directory
	cd demo-gather && make -f Makefile deploy-local --no-print-directory

invoke-local:
	cd demo-scatter && make -f Makefile invoke-local --no-print-directory
	cd demo-segment && make -f Makefile invoke-local --no-print-directory
	cd demo-gather && make -f Makefile invoke-local --no-print-directory

log:
	cd demo-scatter && make -f Makefile log --no-print-directory
	cd demo-segment && make -f Makefile log --no-print-directory
	cd demo-gather && make -f Makefile log --no-print-directory

download:
	aws s3 --endpoint-url=http://localhost:4566 \
	cp s3://aggregatebucket/ ./result --exclude "*" \
	--include "*.xlsx" --recursive

stepfunction:
	aws stepfunctions create-state-machine \
		--name Aggregate \
		--definition file://parallel.json \
		--role-arn "arn:aws:iam::000000000000:role/DummyRole" \
		--endpoint http://localhost:4566

	aws stepfunctions start-execution \
		--state-machine arn:aws:states:us-east-1:000000000000:stateMachine:Aggregate \
		--endpoint http://localhost:4566

	aws stepfunctions delete-state-machine \
		--state-machine-arn "arn:aws:states:us-east-1:000000000000:stateMachine:Aggregate" \
		--endpoint=http://localhost:4566
