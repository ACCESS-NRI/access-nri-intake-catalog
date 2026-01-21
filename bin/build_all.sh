#!/bin/bash -l

#PBS -P iq82
#PBS -l storage=gdata/xp65+gdata/ik11+gdata/cj50+gdata/p73+gdata/dk92+gdata/al33+gdata/rr3+gdata/fs38+gdata/oi10+gdata/hq89+gdata/py18+gdata/ig45+gdata/zz63+gdata/rt52+gdata/jk72+gdata/qv56+gdata/ct11+gdata/ol01+gdata/bs94+gdata/av17
#PBS -q normal
#PBS -W block=false
#PBS -l walltime=06:00:00
#PBS -l mem=192gb
#PBS -l ncpus=48
#PBS -l jobfs=10gb
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
module load conda/analysis3-latest
module load openmpi

export PYTHONTRACEMALLOC=1

EXPECTED_BRANCH=main
if [ "`git branch --show-current`" != "$EXPECTED_BRANCH" ]; then
    echo "Current Git branch is not \"$EXPECTED_BRANCH\"."
    echo "Consider running 'git checkout main' or updating EXPECTED_BRANCH in this script."
    exit 1
fi

OUTPUT_BASE_PATH=/g/data/xp65/public/apps/access-nri-intake-catalog
CONFIG_DIR=/g/data/xp65/admin/access-nri-intake-catalog/config
CONFIGS=( cmip5.yaml cmip6.yaml access-om2.yaml access-cm2.yaml access-esm1-5.yaml ccam.yaml barpa.yaml cordex.yaml mom6.yaml narclim2.yaml era5.yaml romsiceshelf.yaml esgf-ref.yaml esmvaltool-obs.yaml woa.yaml aus2200.yaml)

config_paths=( "${CONFIGS[@]/#/${CONFIG_DIR}/}" )

# Check the xp65 config matches that in the repo 
for conf in ${CONFIGS[@]}; do
    xp65_conf=$CONFIG_DIR/$conf
    repo_conf=$(git rev-parse --show-toplevel)/config/$conf
    if ! diff $xp65_conf $repo_conf; then
        echo "Repo and xp65 config yaml are not identical for $conf"
        echo $xp65_conf
        echo $repo_conf
        exit 1
    fi
done

if [ -z "$version" ]; then
    catalog-build --build_base_path=${OUTPUT_BASE_PATH} --catalog_base_path=${OUTPUT_BASE_PATH} ${config_paths[@]}
else
    catalog-build --build_base_path=${OUTPUT_BASE_PATH} --catalog_base_path=${OUTPUT_BASE_PATH} --version=${version} ${config_paths[@]}
fi
