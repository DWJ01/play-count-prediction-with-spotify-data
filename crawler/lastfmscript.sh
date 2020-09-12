#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --time=30:00:00
#SBATCH --mem=30GB
#SBATCH --job-name=SpotifyAPICrawler
#SBATCH --mail-type=END
#SBATCH --mail-user=wd633@nyu.edu
#SBATCH --output=slurm_%j.out
 
module purge
module load python3/intel/3.7.3

RUNDIR=$SCRATCH/spotifycrawler/run
mkdir -p $RUNDIR
 
cp /home/wd633/spotifycrawler/get_features.py $RUNDIR
 
cd $RUNDIR
python get_features.py