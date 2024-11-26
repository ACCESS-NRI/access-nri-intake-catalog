#!/bin/bash -l

#PBS -P iq82
#PBS -l storage=gdata/xp65+gdata/ik11+gdata/cj50+gdata/hh5+gdata/p73+gdata/dk92+gdata/al33+gdata/rr3+gdata/fs38+gdata/oi10
#PBS -q normal
#PBS -W block=true
#PBS -l walltime=00:30:00
#PBS -l mem=32gb
#PBS -l ncpus=12
#PBS -l wd
#PBS -j oe

########################################################################################### 
# Copyright 2022 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

# Description:
#   Generate access-nri intake metacatalog from config files

###########################################################################################

set -e

module use /g/data/xp65/public/modules
module load conda/access-med-0.6

pytest -s --e2e tests
