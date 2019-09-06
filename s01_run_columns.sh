#!/bin/bash
START_DATE=`date`
echo "Job started at: $START_DATE"
#cwd=$(pwd)
#cwd=/x/home/rmurugaiyan/new_proj/projects/p05_dbtoolkit/m05_dbtoolkit
cwd=/etl/finance/prod/pp_fact_critical_batch/load_framework_home/projects/p05_dbtoolkit/m05_dbtoolkit
#export PATH=/etl/LVS/dmetldata11/scaas/anaconda3/bin:$PATH                                  
export PATH=/usr/local/miniconda-2.7.13/bin:$PATH
#export PYTHONPATH=$cwd:$cwd/src/main:$cwd/src/test:$cwd/src:/x/home/hvenkat/python-libs/getent-0.2:/x/home/hvenkat/python-libs/sshtunnel-0.1.4:/x/home/hvenkat/python-libs/netmiko-2.2.2

export PYTHONPATH=$cwd:$cwd/src/main:$cwd/src/test:/etl/LVS/dmetldata11/scaas/anaconda3/bin:$PATH

#:$cwd/src:/x/home/hvenkat/python-libs/getent-0.2:/x/home/hvenkat/python-libs/sshtunnel-0.1.4:/x/home/hvenkat/python-libs/netmiko-2.2.2
python $cwd/src/driver/run_columns.py
END_DATE=`date`

echo "Job started at: $START_DATE"
echo "Job started at: $END_DATE"

sdt=$( date -d "$START_DATE" +%s )
edt=$( date -d "$END_DATE" +%s )

echo "$edt $sdt" | awk ' { printf("It took %.2f hours to complete this job.\n", ($1-$2)/60/60); } '


