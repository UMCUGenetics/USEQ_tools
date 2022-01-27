from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from xml.dom.minidom import parse
from config import FILE_STORAGE, SQLALCHEMY_DATABASE_URI

import os
import glob
import sys

def link_results(run_id, run_dir):
    flowcell_id = None
    illumina_info = os.path.join(run_dir, 'RunInfo.xml')
    run_name = os.path.basename(os.path.normpath(run_dir))
    nanopore_info = glob.glob('final_summary_*.txt')

    if os.path.exists( illumina_info ):
        # Illumina run
        run_parameters = parse(os.path.join(run_dir, 'RunParameters.xml'))
        experiment_name = run_parameters.getElementsByTagName('ExperimentName')[0].firstChild.nodeValue
        if experiment_name != run_id:
            sys.exit(f'ExperimentName ins RunParameters.xml does not match {run_id}')

        run_info = parse(illumina_info)
        flowcell_id = run_info.getElementsByTagName('Flowcell')[0].firstChild.nodeValue
        stats_dir = os.path.join(run_dir, 'Data/Intensities/BaseCalls/Stats')

        with open( os.path.join(stats_dir, 'multiqc_data/multiqc_bcl2fastq_bysample.json'), 'r') as s:
            general_stats = s.read()

        # mkdir remotely ssh apenboom mkdir -p existingdir/newdir
        os.system(f"mkdir -p {FILE_STORAGE}/{run_id.upper()}/{flowcell_id.upper()}")
        rsync_command = f'/usr/bin/rsync {stats_dir}/multiqc_plots/png/mqc_bcl2fastq_*.png {stats_dir}/*png {FILE_STORAGE}/{run_id.upper()}/{flowcell_id.upper()}'
        exit_code = os.system(rsync_command)
        if exit_code:
            sys.exit(f'Failed to copy plots to {FILE_STORAGE}')

        run = session.query(Run).filter_by(run_id=run_id).all()[0]
        ill_stats = IlluminaSequencingStats(
            flowcell_id=flowcell_id,
            general_stats=general_stats,
            flowcell_intensity_plot = f'{run_name}_flowcell-Intensity.png',
            flowcell_density_plot = f'{run_name}_Clusters-by-lane.png',
            total_qscore_lanes_plot = f'{run_name}_q-histogram.png',
            cycle_qscore_lanes_plot = f'{run_name}_q-heat-map.png',
            cycle_base_plot = f'{run_name}_BasePercent-by-cycle_BasePercent.png',
            cycle_intensity_plot = f'{run_name}_Intensity-by-cycle_Intensity.png',
            lane_counts_plot = 'mqc_bcl2fastq_lane_counts_1.png',
            sample_counts_plot = 'mqc_bcl2fastq_sample_counts_Index_mismatches.png',
            undetermined_plot = 'mqc_bcl2fastq_undetermined_1.png',
            run_id=run.id
        )
        session.add(ill_stats)
        session.commit()
#
# class IlluminaSequencingStats(db.Model):
#     id = db.Column(db.Integer, primary_key=True)
#     flowcell_id = db.Column(db.String(20), index=True)
#     general_stats = db.Column(db.Text())
#
#     # raw seq stats
#     flowcell_intensity_plot = db.Column(db.String(200))
#     flowcell_density_plot = db.Column(db.String(200))
#     total_qscore_lanes_plot = db.Column(db.String(200))
#     cycle_qscore_lanes_plot = db.Column(db.String(200))
#     cycle_base_plot = db.Column(db.String(200))
#     cycle_intensity_plot = db.Column(db.String(200))
#
#     # conversion stats
#     lane_counts_plot = db.Column(db.String(200))
#     sample_counts_plot = db.Column(db.String(200))
#     undetermined_plot = db.Column(db.String(200))
#
#     run_id = db.Column(db.Integer, db.ForeignKey('run.id'))



    elif nanopore_info:
        # Nanopore run
        flowcell_id = nanopore_info[0].split('_')[2]
    else:
        sys.exit('Could not find a RunInfo.xml or final_summary_*.txt file')


def run(run_id,run_dir):

    global session
    # global User
    global Run
    global IlluminaSequencingStats

    Base = automap_base()

    # engine, suppose it has two tables 'user' and 'run' set up
    engine = create_engine(SQLALCHEMY_DATABASE_URI)

    # reflect the tables
    Base.prepare(engine, reflect=True)

    # mapped classes are now created with names by default
    # matching that of the table name.
    # User = Base.classes.user
    Run = Base.classes.run
    print(dir(Base.classes))
    IlluminaSequencingStats = Base.classes.illumina_sequencing_stats

    session = Session(engine)
    link_results(run_id, run_dir)
# if __name__ == "__main__":
#     run_id = sys.argv[1]
#     run_dir = sys.argv[2]
