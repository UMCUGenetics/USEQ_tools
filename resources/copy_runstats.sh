#!/bin/sh

cd /hpc/cog_bioinf/ubec/useq/raw_data
DIRS="novaseq_umc01 miseq_umc01 nextseq_umc03 nextseq_umc04 nextseq_umc05 nextseq2000/nextseq2000_umc01 nextseq2000/nextseq2000_umc02 nextseq2000_umc01 nextseq2000_umc02"
for dir in $DIRS;do
 for rundir in $(find $dir -mindepth 1 -maxdepth 1 -type d );do
  runname=$(basename $rundir);
  machine=$(echo $runname | awk '{split($1, parts, "_"); print parts[2]}');
  #mkdir -p /data/isi/b/bioinf_dna_archive/cuppen/projects/TP0002_USEQ/general/analysis/sboymans/runstats/$machine/$runname;
  mkdir -p /hpc/cuppen/projects/TP0002_USEQ/general/analysis/sboymans/runstats/$machine/$runname;
  #rsync -a --include '*_summary.csv' --include '*ConversionStats.xml' --exclude "*" $rundir/Data/Intensities/BaseCalls/Stats/ /data/isi/b/bioinf_dna_archive/cuppen/projects/TP0002_USEQ/general/analysis/sboymans/runstats/$machine/$runname;
  rsync -a --include '*_summary.csv' --include '*ConversionStats.xml' --exclude "*" $rundir/Data/Intensities/BaseCalls/Stats/ /hpc/cuppen/projects/TP0002_USEQ/general/analysis/sboymans/runstats/$machine/$runname;
 done
done
