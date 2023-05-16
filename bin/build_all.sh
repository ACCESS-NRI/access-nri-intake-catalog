#!/bin/bash -l

#PBS -P tm70
#PBS -l storage=gdata/tm70+gdata/ik11+gdata/cj50+gdata/hh5+gdata/p73+gdata/dk92
#PBS -q normal
#PBS -l walltime=02:00:00
#PBS -l mem=192gb
#PBS -l ncpus=48
#PBS -l wd
#PBS -j oe

##################################################### 
# Copyright 2022 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

# Description:
#   Update all intake catalogs from config files
#
#####################################################

set -e

conda activate access-nri-intake-dev

OUTPUT_BASE_PATH=/g/data/tm70/intake
CONFIG_DIR=/g/data/tm70/ds0092/projects/access-nri-intake-catalog/config
CONFIGS=( cmip6.yaml cmip5.yaml access-om2.yaml access-cm2.yaml access-esm1-5.yaml ) # erai.yaml

# Get current version and set up the directories
version=$(python ../setup.py --version)
version_path=${OUTPUT_BASE_PATH}/v${version}
build_path=${version_path}/sources
mkdir ${version_path}
mkdir ${build_path}

metacatalog_file=${version_path}/metacatalog.csv
config_paths=( "${CONFIGS[@]/#/${CONFIG_DIR}/}" )

metacat-build --build_path=${build_path} --metacatalog_file=${metacatalog_file} ${config_paths[@]}

