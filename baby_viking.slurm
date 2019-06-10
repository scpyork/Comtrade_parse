#!/bin/bash
#SBATCH --job-name=COMTRADE
#SBATCH --output=/tmp/arrayJob_%A_%a.out
#SBATCH --error=/tmp/arrayJob_%A_%a.err
#SBATCH --time=48:00:00
#SBATCH --cpus-per-task=20
#SBATCH --mem-per-cpu=10GB
#SBATCH --mail-user=dp626@york.ac.uk
#SBATCH --mail-type=END,FAIL
cd "${SLURM_SUBMIT_DIR}" || exit 1
export OMP_NUM_THREADS="${SLURM_CPUS_PER_TASK}"

source .delay_bash_profile

source activate dsmacc-env
######################
# Begin work section #
######################

# Print this sub-job's task ID
echo "My SLURM_ARRAY_TASK_ID: " $SLURM_ARRAY_TASK_ID

# Do some work based on the SLURM_ARRAY_TASK_ID
# For example:
# ./my_process $SLURM_ARRAY_TASK_ID
#
# where my_process is you executable

echo ${MYPROG} $SLURM_ARRAY_TASK_ID
ipython ${MYPROG} $SLURM_ARRAY_TASK_ID
