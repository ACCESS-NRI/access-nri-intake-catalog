#!/bin/bash -l

#PBS -P iq82
#PBS -l storage=gdata/tm70+gdata/xp65+gdata/ik11+gdata/cj50+gdata/hh5+gdata/p73+gdata/dk92
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

OUTPUT_BASE_PATH=/g/data/xp65/public/apps/access-nri-intake-catalog
CONFIG_DIR=/g/data/xp65/admin/access-nri-intake-catalog/config
CONFIGS=( cmip5.yaml cmip6.yaml access-om2.yaml access-cm2.yaml access-esm1-5.yaml ) # erai.yaml

config_paths=( "${CONFIGS[@]/#/${CONFIG_DIR}/}" )

if [ -z "$version" ]; then
    catalog-build --build_base_path=${OUTPUT_BASE_PATH} ${config_paths[@]}
else
    catalog-build --build_base_path=${OUTPUT_BASE_PATH} --version=${version} ${config_paths[@]}
fi
