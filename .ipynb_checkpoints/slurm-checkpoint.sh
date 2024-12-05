#!/bin/bash
#SBATCH --job-name=GOES_16_ref_gen
#SBATCH --output=/discover/nobackup/edlang1/aist_slurm_outputs/output_%j.log
#SBATCH --error=/discover/nobackup/edlang1/aist_slurm_outputs/error_%j.log
#SBATCH --constraint=mil
#SBATCH --constraint=cssro
#SBATCH --account=s2826
#SBATCH --time=07:00:00

source /gpfsm/dhome/edlang1/.bashrc

echo "Starting script"

pixi shell

for band in 1 2 14
do
	for day in {260..290}
	do
		python create.py -y 2019 -b $band -d $day -n 16
	done
done

echo "Finishing Script"
