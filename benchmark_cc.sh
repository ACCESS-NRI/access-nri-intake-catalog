#!/bin/bash -l

#PBS -P tm70
#PBS -l storage=gdata/tm70+gdata/ik11+gdata/cj50+gdata/hh5
#PBS -q normal
#PBS -l walltime=48:00:00
#PBS -l mem=192gb
#PBS -l ncpus=48
#PBS -l wd
#PBS -j oe

module use /g/data/hh5/public/modules
module load conda/analysis3-unstable

python benchmark.py --database="cc"
