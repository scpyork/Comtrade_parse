#! /bin/bash
ngroups=$(wc -l read.files | cut -d " " -f1)
rm /tmp/*.out
rm /tmp/*.err


echo 'reading'
echo ${ngroups}

SLURM_ARRAY_TASK_MAX=${ngroups} MYPROG='correct.py' sbatch --array=0-${ngroups} baby_viking.slurm
#https://slurm.schedmd.com/job_array.html
#scancel -u <username>
#SBATCH --nodes=100
#SBATCH --ntasks=10
