# StepFunctions Demo
LocalStackでStepFunctionsを実行するデモスクリプトです。\
動的並列処理をStepFunctionsで実装しています。

## システム構成図
![システム構成図](/img/システム構成図.png)

## Usage
### LocalStackの起動

```bash
docker-compose up
```

### Lambdaのzip化
```bash
make zip
```

### Lambdaの初回デプロイ
```bash
make create
```

### Lambdaの更新
```bash
make update
```

### Lambdaの削除
```bash
make delete
```

### Stepfunctionsの実行
```bash
make stepfunctions
```
### S3にアップロードしたエクセルファイルのダウンロード
resultフォルダに格納されます。
```bash
make download
```

### テスト
```bash
make test
```

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
