#!/bin/bash

set -e

cpus=( 3 6 12 24 36 48 96 144 )

prev_job_id=`qsub -l ncpus=1,mem=12gb build_all.sh`
for cpu in ${cpus[@]}; do
    mem=$(($cpu * 4))
    prev_job_id=`qsub -W depend=afterany:${prev_job_id} -l ncpus=${cpu},mem=${mem}gb build_all.sh`
done
    

