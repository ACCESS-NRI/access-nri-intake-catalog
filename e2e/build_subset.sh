#!/bin/bash -l

#PBS -P iq82
#PBS -l storage=gdata/xp65+gdata/ik11+gdata/cj50+gdata/hh5+gdata/p73+gdata/dk92+gdata/al33+gdata/rr3+gdata/fs38+gdata/oi10
#PBS -q normal
#PBS -W block=true
#PBS -l walltime=03:00:00
#PBS -l mem=192gb
#PBS -l ncpus=48
#PBS -l wd
#PBS -j oe

########################################################################################### 
# Copyright 2022 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

# Description:
#   Generate access-nri intake metacatalog from config files

###########################################################################################

set -e

if [ ! $# -eq 0 ]; then
    version=$1
fi

module use /g/data/xp65/public/modules
module load conda/access-med-0.6
source /home/189/ct1163/end2end/venv/bin/activate

OUTPUT_BASE_PATH=/scratch/tm70/ct1163/test_cat/
CONFIG_DIR=/g/data/xp65/admin/access-nri-intake-catalog/config
CONFIGS=( cmip5.yaml access-om2.yaml )

config_paths=( "${CONFIGS[@]/#/${CONFIG_DIR}/}" )

if [ -z "$version" ]; then
    catalog-build --build_base_path=${OUTPUT_BASE_PATH} ${config_paths[@]}

else
    catalog-build --build_base_path=${OUTPUT_BASE_PATH} ${config_paths[@]}
fi
