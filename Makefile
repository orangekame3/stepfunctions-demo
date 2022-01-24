.PHONY: zip delete create update invoke log download stepfunction test
zip:
	cd demo-scatter && make -f Makefile zip --no-print-directory
	cd demo-segment && make -f Makefile zip --no-print-directory
	cd demo-gather && make -f Makefile zip --no-print-directory

delete:
	cd demo-scatter && make -f Makefile delete --no-print-directory
	cd demo-segment && make -f Makefile delete --no-print-directory
	cd demo-gather && make -f Makefile delete --no-print-directory

create:
	cd demo-scatter && make -f Makefile create --no-print-directory
	cd demo-segment && make -f Makefile create --no-print-directory
	cd demo-gather && make -f Makefile create --no-print-directory

update:
	cd demo-scatter && make -f Makefile update --no-print-directory
	cd demo-segment && make -f Makefile update --no-print-directory
	cd demo-gather && make -f Makefile update --no-print-directory

invoke:
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

bucket:
	aws --endpoint-url=http://localhost:4566 \
		--profile localstack s3api create-bucket \
		--bucket aggregatebucket

	aws s3 --endpoint-url=http://localhost:4566 \
		cp utils/data/sample.json s3://aggregatebucket/data/ \
	 	--profile=localstack

stepfunctions:
	python utils/generate.py 1000

	aws stepfunctions create-state-machine \
		--name Aggregate \
		--definition file://state-machine/parallel.json \
		--role-arn "arn:aws:iam::000000000000:role/DummyRole" \
		--endpoint http://localhost:4566

	aws stepfunctions start-execution \
		--state-machine arn:aws:states:us-east-1:000000000000:stateMachine:Aggregate \
		--endpoint http://localhost:4566

	aws stepfunctions delete-state-machine \
		--state-machine-arn "arn:aws:states:us-east-1:000000000000:stateMachine:Aggregate" \
		--endpoint=http://localhost:4566

test:
	cd demo-scatter && make -f Makefile test --no-print-directory
