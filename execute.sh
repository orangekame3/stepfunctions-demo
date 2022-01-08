#!/bin/bash
ROOT=$(pwd)
#scatter
cd demo-scatter
sh execute.sh
#segment
cd $ROOT
cd demo-segment
sh execute.sh
# gather
cd $ROOT
cd demo-gather
sh execute.sh
