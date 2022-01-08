#!/bin/bash

# pipenv 仮想環境を起動してから実行する
ROOT=$(pwd)

#scatter
cd demo-scatter
sh zip.sh
#segment
cd $ROOT
cd demo-segment
sh zip.sh
# gather
cd $ROOT
cd demo-gather
sh zip.sh
