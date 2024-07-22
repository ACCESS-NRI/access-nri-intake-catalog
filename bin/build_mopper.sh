#!/bin/bash -l

#PBS -P v45
#PBS -l storage=gdata/hh5+gdata/ua8+scratch/v45
#PBS -q normal
#PBS -l walltime=00:30:00
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

module load conda/analysis3
conda activate access-nri-intake-test

OUTPUT_BASE_PATH=/g/data/ua8/Working/packages/access-nri-intake-catalog
CONFIG_DIR=/g/data/ua8/Working/packages/access-nri-intake-catalog/config
CONFIGS=( access-mopper.yaml ) # erai.yaml

config_paths=( "${CONFIGS[@]/#/${CONFIG_DIR}/}" )

if [ -z "$version" ]; then
    catalog-build --build_base_path=${OUTPUT_BASE_PATH} ${config_paths[@]}
else
    catalog-build --build_base_path=${OUTPUT_BASE_PATH} --version=${version} ${config_paths[@]}
fi
