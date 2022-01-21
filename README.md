# StepFunctions Demo
ローカルスタックでStepFunctionsを実行するデモスクリプトです。
動的並列処理をStepFunctionsで実装しています。

## ディレクトリ構造

```bash
.
├── Makefile
├── README.md
├── demo-gather
│   ├── Makefile
│   ├── Pipfile
│   ├── Pipfile.lock
│   ├── bin
│   │   └── lambda.zip
│   ├── gather.py
│   ├── lambda.py
│   ├── result.log
│   └── tests
├── demo-scatter
│   ├── Makefile
│   ├── Pipfile
│   ├── Pipfile.lock
│   ├── bin
│   │   └── lambda.zip
│   ├── lambda.py
│   ├── result.log
│   ├── scatter.py
│   └── tests
│       ├── __init__.py
│       └── test_scatter.py
├── demo-segment
│   ├── Makefile
│   ├── Pipfile
│   ├── Pipfile.lock
│   ├── bin
│   │   └── lambda.zip
│   ├── lambda.py
│   ├── segment.py
│   └── tests
├── docker-compose.yml
├── generate.py
├── parallel.json
├── result
│   └── summary.xlsx
└── sample.json

10 directories, 29 files
```
