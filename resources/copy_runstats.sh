#!/bin/sh

RAW_DATA_PATH=/hpc/useq/raw_data/
USEQ_TOOLS=/hpc/local/CentOS7/cog_bioinf/USEQ_tools-sms/
RUNSTATS_PATH=/hpc/useq/raw_data/runstats/
WEBLOC=sitkaspar:/var/www/html/USEQ-Overview-dev
#GATHER RUN STATS

cd $RAW_DATA_PATH
# DIRS="testruns"
DIRS="novaseq_umc01 miseq_umc01 nextseq_umc03 nextseq2000/nextseq2000_umc01 nextseq2000/nextseq2000_umc02 nextseq2000_umc01 nextseq2000_umc02"
for dir in $DIRS;do
 for rundir in $(find $dir -mindepth 1 -maxdepth 1 -type d );do
  runname=$(basename $rundir);
  machine=$(echo $runname | awk '{split($1, parts, "_"); print parts[2]}');
  echo $runname $machine
  mkdir -p $RUNSTATS_PATH$machine/$runname;
  if [ -d $rundir/Conversion ]; then
    rsync -a --prune-empty-dirs --include '*/' --include '*_summary.csv' --include '*Demultiplex_Stats.csv' --include "*Adapter_Metrics.csv" --exclude "*" $rundir/Conversion/Reports/ $RUNSTATS_PATH$machine/$runname;
  else
    rsync -a --include '*_summary.csv' --include '*ConversionStats.xml' --exclude "*" $rundir/Data/Intensities/BaseCalls/Stats/ $RUNSTATS_PATH$machine/$runname;
  fi

 done
done

#UPDATE RUN OVERVIEW
cd $USEQ_TOOLS
. env/bin/activate

python useq_tools.py daemons run_overview -o $RUNSTATS_PATH/overview.json

#COPY UPDATED OVERVIEW TO WEBHOST
scp -i ~/.ssh/sitkaspar-id $RUNSTATS_PATH/overview* $WEBLOC
