#!/bin/bash
ROOT=$(pwd)
#scatter
cd demo-scatter
sh deploy.sh
#segment
cd $ROOT
cd demo-segment
sh deploy.sh
# gather
cd $ROOT
cd demo-gather
sh deploy.sh
