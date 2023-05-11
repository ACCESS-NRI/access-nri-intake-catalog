#!/bin/bash -l

#PBS -P tm70
#PBS -l storage=gdata/tm70+gdata/ik11+gdata/cj50+gdata/hh5+gdata/p73+gdata/dk92
#PBS -q express
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

# Path to metacatalog
CATALOG_NAME=/g/data/tm70/intake/dfcatalog.csv

# Path to location of config YAML files
CONFIG_DIR=/g/data/tm70/ds0092/projects/access-nri-intake-catalog/config

# Config files to process
CONFIGS=( cmip6.yaml cmip5.yaml access-om2.yaml access-cm2.yaml access-esm1-5.yaml ) # erai.yaml

conda activate access-nri-intake-dev
config_paths=( "${CONFIGS[@]/#/${CONFIG_DIR}/}" )
metacat-build --catalog_name=${CATALOG_NAME} ${config_paths[@]}

