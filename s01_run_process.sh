#!/bin/bash

#cwd=$(pwd)
#cwd=/x/home/rmurugaiyan/new_proj/projects/p05_dbtoolkit/m05_dbtoolkit
cwd=/etl/finance/prod/pp_fact_critical_batch/load_framework_home/projects/p05_dbtoolkit/m05_dbtoolkit
#export PATH=/etl/LVS/dmetldata11/scaas/anaconda3/bin:$PATH                                  
export PATH=/usr/local/miniconda-2.7.13/bin:$PATH
#export PYTHONPATH=$cwd:$cwd/src/main:$cwd/src/test:$cwd/src:/x/home/hvenkat/python-libs/getent-0.2:/x/home/hvenkat/python-libs/sshtunnel-0.1.4:/x/home/hvenkat/python-libs/netmiko-2.2.2
export PYTHONPATH=$cwd:$cwd/src/main:$cwd/src/test
#:$cwd/src:/x/home/hvenkat/python-libs/getent-0.2:/x/home/hvenkat/python-libs/sshtunnel-0.1.4:/x/home/hvenkat/python-libs/netmiko-2.2.2

python $cwd/src/driver/run_process.py

