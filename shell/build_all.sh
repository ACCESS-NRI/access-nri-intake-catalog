#!/bin/bash -l

#PBS -P tm70
#PBS -l storage=gdata/tm70+gdata/ik11+gdata/cj50+gdata/hh5+gdata/p73+gdata/dk92
#PBS -q express
#PBS -l walltime=02:00:00
#PBS -l mem=192gb
#PBS -l ncpus=48
#PBS -l jobfs=10GB
#PBS -l wd
#PBS -j oe

##################################################### 
# Copyright 2022 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

# Description:
#
# Update all intake catalogs from config files
#
#####################################################

conda activate catalog-manager-dev

CONFIG_DIR=/g/data/tm70/ds0092/projects/nri_intake_catalog/config
CATALOG_DIR=/g/data/tm70/ds0092/projects/nri_intake_catalog/catalogs
CATALOG_NAME=dfcatalog

configs=( cmip6.yaml access-om2.yaml access-cm2.yaml access-esm1-5.yaml )

for config in "${configs[@]}"; do
    buildcat --catalog_dir=${CATALOG_DIR} --catalog_name=${CATALOG_NAME} ${CONFIG_DIR}/$config
done
