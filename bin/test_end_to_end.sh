#!/bin/bash -l

#PBS -P iq82
#PBS -l storage=gdata/xp65+gdata/ik11+gdata/cj50+gdata/p73+gdata/dk92+gdata/al33+gdata/rr3+gdata/fs38+gdata/oi10+gdata/hq89+gdata/py18+gdata/ig45+gdata/zz63+gdata/rt52+gdata/jk72+gdata/qv56+gdata/ct11
#PBS -q normal
#PBS -W block=true
#PBS -l walltime=00:30:00
#PBS -l mem=64gb
#PBS -l ncpus=12
#PBS -l wd
#PBS -j oe
#PBS -W umask=0022

########################################################################################### 
# Copyright 2022 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

# Description:
#   Generate access-nri intake metacatalog from config files

###########################################################################################

set -e

module use /g/data/xp65/public/modules
module load conda/analysis3-25.05 # THIS NEEDS TO BE UPDATED TO THE LATEST VERSION
module load openmpi

pytest -s --e2e /g/data/xp65/admin/access-nri-intake-catalog/tests/e2e
