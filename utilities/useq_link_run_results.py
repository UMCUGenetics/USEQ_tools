from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from xml.dom.minidom import parse
from config import FILE_STORAGE, SQLALCHEMY_DATABASE_URI,STAT_PATH,RUN_PROCESSES, DATA_DIRS_RAW
from genologics.entities import Project
from pathlib import Path
import os
import glob
import sys
import xml.etree.cElementTree as ET
import csv
# total_reads
# total_mean_qual
# total_q30
#
#
# 'Index' : None,
# '# Reads' : 0,
# '# Perfect Index Reads' : 0,
# '# One Mismatch Index Reads' : 0,
# Mean Quality Score (PF)
# q30 = f'Read  % Q30'


def parseConversionStatsType1( conversion_stats_file ):
    total_yield = 0

    stats = {
        'total_reads' : 0,
        'total_mean_qual' : 0,
        'total_q30' : 0,
        'samples' : [
        ],
    }
    def getelements(filename_or_file, tag):
        context = iter(ET.iterparse(filename_or_file, events=('start', 'end')))
        _, root = next(context) # get root element
        # print(root)
        for event, elem in context:
            # print(event,elem)
            if event == 'end' and elem.tag == tag:
                yield elem
                root.clear() # preserve memory


    for sample in getelements(conversion_stats_file, "Sample"):
        if sample.attrib['name'] == 'all':continue
        sample_yield1 = 0
        sample_yield2 = 0
        sample_stats = {
            'SampleID' : sample.attrib['name'],
            'Index' : sample.find("./Barcode").attrib['name'],
            '# Reads' : 0,
            '# Perfect Index Reads' : None,
            '# One Mismatch Index Reads' :None,
            'Read 1 Mean Quality Score (PF)' : 0,
            'Read 2 Mean Quality Score (PF)' : 0,
            'Read 1 % Q30': 0,
            'Read 2 % Q30': 0,
            'Read I1 % Q30': 0,
            'Read I2 % Q30': 0,
        }


        for tile in sample.iter("Tile"):

            pf_counts = tile.find("./Pf/ClusterCount")
            pf_read1_counts = tile.find("./Pf/Read[@number='1']")
            pf_read2_counts = tile.find("./Pf/Read[@number='2']")

            #sample
            sample_stats['# Reads'] += int(pf_counts.text)
            sample_yield1 += int(pf_read1_counts.find("Yield").text)
            sample_stats['Read 1 Mean Quality Score (PF)'] +=int(pf_read1_counts.find("QualityScoreSum").text)
            sample_stats['Read 1 % Q30'] += int(pf_read1_counts.find("YieldQ30").text)
            #totals
            stats['total_reads'] += int(pf_counts.text)
            total_yield += int(pf_read1_counts.find("Yield").text)
            stats['total_q30'] += int(pf_read1_counts.find("YieldQ30").text)
            stats['total_mean_qual'] += int(pf_read1_counts.find("QualityScoreSum").text)

            if pf_read2_counts is not None:
                total_yield += int(pf_read2_counts.find("Yield").text)
                stats['total_q30'] += int(pf_read2_counts.find("YieldQ30").text)
                stats['total_mean_qual'] += int(pf_read2_counts.find("QualityScoreSum").text)

                sample_yield2 += int(pf_read2_counts.find("Yield").text)
                sample_stats['Read 2 Mean Quality Score (PF)'] +=int(pf_read2_counts.find("QualityScoreSum").text)
                sample_stats['Read 2 % Q30'] += int(pf_read2_counts.find("YieldQ30").text)

        if sample_yield1: sample_stats['Read 1 Mean Quality Score (PF)'] = sample_stats['Read 1 Mean Quality Score (PF)'] / float(sample_yield1)
        if sample_yield2: sample_stats['Read 2 Mean Quality Score (PF)'] = sample_stats['Read 2 Mean Quality Score (PF)'] / float(sample_yield2)
        if sample_yield1: sample_stats['Read 1 % Q30'] = (sample_stats['Read 1 % Q30'] / float(sample_yield1)) * 100
        sample_stats['# Reads'] = sample_stats['# Reads']/2
        if sample_yield2: sample_stats['Read 2 % Q30'] = (sample_stats['Read 2 % Q30'] / float(sample_yield2)) * 100

        stats['samples'].append(sample_stats)
    if total_yield: stats['total_q30'] = (stats['total_q30'] / float( total_yield )) * 100
    if total_yield: stats['total_mean_qual'] = stats['total_mean_qual'] / float( total_yield )

    stats['total_reads'] = stats['total_reads']/2

    return stats


def parseConversionStatsType2(lims, pid, qual_metrics,demux_stats):
    # print(demux_stats,qual_metrics)
    samples = lims.get_samples(projectlimsid=pid)
    sample_names = [x.name for x in samples]
    stats = {
        'total_reads' : 0,
        'total_mean_qual' : 0,
        'total_q30' : 0,
        'samples' : [
        ],
    }
    samples_tmp = {}
    with open(demux_stats, 'r') as d:
        csv_reader = csv.DictReader(d)
        for row in csv_reader:
            # print(row)
            stats['total_reads'] += float(row['# Reads'])

            if row['SampleID'] not in samples_tmp:
                samples_tmp[ row['SampleID'] ] = {
                    'Index' : None,
                    '# Reads' : 0,
                    '# Perfect Index Reads' : 0,
                    '# One Mismatch Index Reads' : 0,
                    'Read 1 Mean Quality Score (PF)' : 0,
                    'Read 2 Mean Quality Score (PF)' : 0,
                    'Read 1 % Q30': 0,
                    'Read 2 % Q30': 0,
                    'Read I1 % Q30': 0,
                    'Read I2 % Q30': 0,
                }
            samples_tmp[ row['SampleID'] ]['Index'] = row['Index']
            samples_tmp[ row['SampleID'] ]['Lane'] = int(row['Lane'])
            samples_tmp[ row['SampleID'] ]['# Reads'] += int(row['# Reads'])
            samples_tmp[ row['SampleID'] ]['# Perfect Index Reads']  += int(row['# Perfect Index Reads'])
            samples_tmp[ row['SampleID'] ]['# One Mismatch Index Reads']  += int(row['# One Mismatch Index Reads'])
            # stats['samples'].append(row)
    if Path(qual_metrics).is_file():

        with open(qual_metrics,'r') as q:
            csv_reader = csv.DictReader(q)
            for row in csv_reader:
                # print(row)
                # print(row['Mean Quality Score (PF)'], row['% Q30'])
                mqs = f'Read {row["ReadNumber"]} Mean Quality Score (PF)'
                q30 = f'Read {row["ReadNumber"]} % Q30'
                if mqs not in samples_tmp[ row['SampleID'] ]:
                    samples_tmp[ row['SampleID'] ][mqs] = 0
                if q30 not in samples_tmp[ row['SampleID'] ]:
                    samples_tmp[ row['SampleID'] ][q30] = 0


                samples_tmp[ row['SampleID'] ][mqs] += float(row['Mean Quality Score (PF)'])
                # print(row['SampleID'], q30, samples_tmp[ row['SampleID'] ][q30], row['% Q30'])
                samples_tmp[ row['SampleID'] ][q30] += float(row['% Q30'])

    for sampleID in samples_tmp:
        if sampleID not in sample_names:continue
        sample = {}
        for read_number in ['1','2','I1','I2']:
            if f'Read {read_number} Mean Quality Score (PF)' in samples_tmp[sampleID]:
                sample[f'Read {read_number} Mean Quality Score (PF)'] = samples_tmp[sampleID][f'Read {read_number} Mean Quality Score (PF)'] / samples_tmp[ row['SampleID'] ]['Lane']
            if f'Read {read_number} % Q30' in samples_tmp[sampleID]:
                sample[f'Read {read_number} % Q30'] = (samples_tmp[sampleID][f'Read {read_number} % Q30'] / samples_tmp[ row['SampleID'] ]['Lane'])*100
        sample['SampleID'] = sampleID
        sample['Index'] = samples_tmp[sampleID]['Index']
        sample['# Reads'] = samples_tmp[sampleID]['# Reads']
        sample['# Perfect Index Reads'] = samples_tmp[sampleID]['# Perfect Index Reads']
        sample['# One Mismatch Index Reads'] = samples_tmp[sampleID]['# One Mismatch Index Reads']
        stats['samples'].append(sample)

    return stats


def parseConversionStatsType3(lims, pid, adapter_metrics, demux_stats):
    samples = lims.get_samples(projectlimsid=pid)
    sample_names = [x.name for x in samples]
    stats = {
        'total_reads' : 0,
        'total_mean_qual' : 0,
        'total_q30' : 0,
        'samples' : [
        ],
    }
    samples_tmp = {}

    # r1_bases = 0
    # r2_bases = 0
    with open(adapter_metrics, 'r') as a:
        csv_reader = csv.DictReader(a)
        for row in csv_reader:
            if row['Sample_ID'] not in samples_tmp:
                samples_tmp[ row['Sample_ID'] ] = {
                    'Index' : None,
                    '# Reads' : 0,
                    '# Perfect Index Reads' : 0,
                    '# One Mismatch Index Reads' : 0,
                    'Read 1 Mean Quality Score (PF)' : 0,
                    'Read 2 Mean Quality Score (PF)' : 0,
                    'Read 1 % Q30': 0,
                    'Read 2 % Q30': 0,
                    'Read I1 % Q30': 0,
                    'Read I2 % Q30': 0,
                    'Read 1 Bases' : 0,
                    'Read 2 Bases' : 0,
                }

            samples_tmp[ row['Sample_ID'] ]['Read 1 Bases'] += int(row['R1_SampleBases'])
            if int(row['R2_SampleBases']) > 0:
                samples_tmp[ row['Sample_ID'] ]['Read 2 Bases'] += int(row['R2_SampleBases'])
    with open(demux_stats, 'r') as d:
        csv_reader = csv.DictReader(d)
        for row in csv_reader:

            stats['total_reads'] += float(row['# Reads'])

            samples_tmp[ row['SampleID'] ]['Index'] = row['Index']
            samples_tmp[ row['SampleID'] ]['Lane'] = int(row['Lane'])
            samples_tmp[ row['SampleID'] ]['# Reads'] += int(row['# Reads'])
            samples_tmp[ row['SampleID'] ]['# Perfect Index Reads']  += int(row['# Perfect Index Reads'])
            samples_tmp[ row['SampleID'] ]['# One Mismatch Index Reads']  += int(row['# One Mismatch Index Reads'])
            samples_tmp[ row['SampleID'] ]['Read 1 Mean Quality Score (PF)'] += float(row['Mean Quality Score (PF)'])
            samples_tmp[ row['SampleID'] ]['Read 1 % Q30'] += int(row['# of >= Q30 Bases (PF)'])
            if samples_tmp[ row['SampleID'] ]['Read 2 Bases'] > 0:
                samples_tmp[ row['SampleID'] ]['Read 2 Mean Quality Score (PF)'] += float(row['Mean Quality Score (PF)'])
                samples_tmp[ row['SampleID'] ]['Read 2 % Q30'] += int(row['# of >= Q30 Bases (PF)'])


    for sampleID in samples_tmp:
        if sampleID not in sample_names:continue
        sample = {}

        # print (f"SampleID : {sampleID} R1 Q30 Bases : {samples_tmp[sampleID]['Read 1 % Q30']} R1 Bases : {samples_tmp[sampleID]['Read 1 Bases']}")
        for read_number in ['1','2']:
            if f'Read {read_number} Mean Quality Score (PF)' in samples_tmp[sampleID]:
                sample[f'Read {read_number} Mean Quality Score (PF)'] = samples_tmp[sampleID][f'Read {read_number} Mean Quality Score (PF)'] / samples_tmp[ row['SampleID'] ]['Lane']
            if f'Read {read_number} % Q30' in samples_tmp[sampleID]:
                sample[f'Read {read_number} # Q30 Bases'] = samples_tmp[sampleID][f'Read {read_number} % Q30']
        #         r1_bases = samples_tmp[ row['SampleID'] ][f'Read {read_number} Bases']
        #
        #         sample[f'Read {read_number} % Q30'] = (samples_tmp[sampleID][f'Read {read_number} % Q30'] / (r1_bases) )*100
        sample['SampleID'] = sampleID
        sample['Index'] = samples_tmp[sampleID]['Index']
        sample['# Reads'] = samples_tmp[sampleID]['# Reads']
        sample['# Perfect Index Reads'] = samples_tmp[sampleID]['# Perfect Index Reads']
        sample['# One Mismatch Index Reads'] = samples_tmp[sampleID]['# One Mismatch Index Reads']
        stats['samples'].append(sample)

    return stats
def getSequencingResults( lims, project_name):
    """Get the most recent raw run info based on project name and allowed RUN_PROCESSES"""
    runs = []

    project_processes = lims.get_processes(
        projectname=project_name,
        type=RUN_PROCESSES
    )

    for process in project_processes:
        run_id = None
        flowcell_id = None
        if 'Run ID' in process.udf: run_id = process.udf['Run ID']

        if 'Flow Cell ID' in process.udf: flowcell_id = process.udf['Flow Cell ID']

        runs.append({
            'date_run' : process.date_run,
            'run_id' : run_id,
            'flowcell_id' : flowcell_id,
            'run_dir' : None
        })

    if not runs:
        return None



    for run in runs:
        # print(run)
        stats_dir = Path(STAT_PATH)
        if run['run_id']:

            for machine_dir in stats_dir.glob("*"):
                md_path = Path(f'{machine_dir}/{run["run_id"]}')
                if md_path.is_dir():
                    run['run_dir'] = md_path
        elif run['flowcell_id']:
            for machine_dir in stats_dir.glob("*"):
                md_path = Path(machine_dir)
                for run_dir in md_path.glob("*"):
                    if run_dir.name.endswith("_000000000-"+run['flowcell_id']): #MiSeq
                        run['run_dir'] = run_dir
                    elif run_dir.name.endswith("_"+run['flowcell_id']): #NextSeq
                        run['run_dir'] = run_dir
                    elif run_dir.name.endswith("A"+run['flowcell_id']) or run_dir.name.endswith("B"+run['flowcell_id']): #HiSeq
                        run['run_dir'] = run_dir

    return runs



def link_results(lims, run_id):
    runs = []
    if run_id:
        runs = session.query(Run).filter_by(run_id=run_id).all()
    else:
        runs = session.query(Run).all()

    for run in runs:
        # print(run.run_id)
        project = None
        try:
            project = Project(lims, id=run.run_id)
            project_name = project.name
        except:
            print (f"Error : Project ID {run_id} not found!")
            continue

        seq_results = getSequencingResults(lims, project_name)

        if not seq_results : continue

        for seq_result in seq_results:
            if not seq_result['run_dir'] : continue
            run_name = Path(seq_result['run_dir']).name
            conversion_stats_file = Path(f"{seq_result['run_dir']}/ConversionStats.xml")
            demux_stats_file = Path(f"{seq_result['run_dir']}/Demultiplex_Stats.csv")
            qual_metrics_file = Path(f"{seq_result['run_dir']}/Quality_Metrics.csv")
            adapter_metrics_file = Path(f"{seq_result['run_dir']}/Adapter_Metrics.csv")
            summary_stats_file = Path(f"{seq_result['run_dir']}/{ Path(seq_result['run_dir']).name }_summary.csv")
            stats = None
            if conversion_stats_file.is_file():
                stats = parseConversionStatsType1(conversion_stats_file) #bcl2fastq conversion

                # print('opt1', stats)
            elif qual_metrics_file.is_file():
                stats = parseConversionStatsType2(lims, run.run_id, qual_metrics_file,demux_stats_file) #bcl-convert >= 3.9
                # print('opt2', stats)
            elif adapter_metrics_file.is_file():
                stats = parseConversionStatsType3(lims, run.run_id, adapter_metrics_file,demux_stats_file) #bcl-convert < 3.9
                # print('opt3', stats)

            for dir in DATA_DIRS_RAW:
                possible_img_dir = Path(f'{dir}/{run_name}')
                if possible_img_dir.is_dir():
                    base = None
                    if Path(f'{dir}/{run_name}/Data/Intensities/BaseCalls/Stats/{run_name}_summary.csv').is_file():
                        print(f'OLD {dir}/{run_name}/Data/Intensities/BaseCalls/Stats/{run_name}_summary.csv')
                        base = f'{dir}/{run_name}/Data/Intensities/BaseCalls/Stats/{run_name}'

                    elif Path(f'{dir}/{run_name}/Conversion/Reports/{run_name}_summary.csv').is_file():
                        print(f'{dir}/{run_name}/Conversion/Reports/{run_name}_summary.csv')
                        base = f'{dir}/{run_name}/Conversion/Reports/{run_name}'

                    if base:
                        flowcell_intensity_plot = f'{base}_flowcell-Intensity.png',
                        flowcell_density_plot = f'{base}_Clusters-by-lane.png',
                        total_qscore_lanes_plot = f'{base}_q-histogram.png',
                        cycle_qscore_lanes_plot = f'{base}_q-heat-map.png',
                        cycle_base_plot = f'{base}_BasePercent-by-cycle_BasePercent.png',
                        cycle_intensity_plot = f'{base}_Intensity-by-cycle_Intensity.png',



    # flowcell_id = None

    # illumina_info = os.path.join(run_dir, 'RunInfo.xml')
    # run_name = os.path.basename(os.path.normpath(run_dir))
    # nanopore_info = glob.glob('final_summary_*.txt')
    #
    # if os.path.exists( illumina_info ):
    #     # Illumina run
    #     run_parameters = parse(os.path.join(run_dir, 'RunParameters.xml'))
    #     experiment_name = run_parameters.getElementsByTagName('ExperimentName')[0].firstChild.nodeValue
    #     if experiment_name != run_id:
    #         sys.exit(f'ExperimentName ins RunParameters.xml does not match {run_id}')
    #
    #     run_info = parse(illumina_info)
    #     flowcell_id = run_info.getElementsByTagName('Flowcell')[0].firstChild.nodeValue
    #     stats_dir = os.path.join(run_dir, 'Data/Intensities/BaseCalls/Stats')
    #
    #     with open( os.path.join(stats_dir, 'multiqc_data/multiqc_bcl2fastq_bysample.json'), 'r') as s:
    #         general_stats = s.read()
    #
    #     # mkdir remotely ssh apenboom mkdir -p existingdir/newdir
    #     os.system(f"mkdir -p {FILE_STORAGE}/{run_id.upper()}/{flowcell_id.upper()}")
    #     rsync_command = f'/usr/bin/rsync {stats_dir}/multiqc_plots/png/mqc_bcl2fastq_*.png {stats_dir}/*png {FILE_STORAGE}/{run_id.upper()}/{flowcell_id.upper()}'
    #     exit_code = os.system(rsync_command)
    #     if exit_code:
    #         sys.exit(f'Failed to copy plots to {FILE_STORAGE}')
    #
    #     run = session.query(Run).filter_by(run_id=run_id).all()[0]
    #     ill_stats = IlluminaSequencingStats(
    #         flowcell_id=flowcell_id,
    #         general_stats=general_stats,
    #         flowcell_intensity_plot = f'{run_name}_flowcell-Intensity.png',
    #         flowcell_density_plot = f'{run_name}_Clusters-by-lane.png',
    #         total_qscore_lanes_plot = f'{run_name}_q-histogram.png',
    #         cycle_qscore_lanes_plot = f'{run_name}_q-heat-map.png',
    #         cycle_base_plot = f'{run_name}_BasePercent-by-cycle_BasePercent.png',
    #         cycle_intensity_plot = f'{run_name}_Intensity-by-cycle_Intensity.png',
    #         lane_counts_plot = 'mqc_bcl2fastq_lane_counts_1.png',
    #         sample_counts_plot = 'mqc_bcl2fastq_sample_counts_Index_mismatches.png',
    #         undetermined_plot = 'mqc_bcl2fastq_undetermined_1.png',
    #         run_id=run.id
    #     )
    #     session.add(ill_stats)
    #     session.commit()
    #
    # elif nanopore_info:
    #     # Nanopore run
    #     flowcell_id = nanopore_info[0].split('_')[2]
    # else:
    #     sys.exit('Could not find a RunInfo.xml or final_summary_*.txt file')


def run(lims, run_id):

    global session
    # global User
    global Run
    global IlluminaSequencingStats

    Base = automap_base()
# pymysql.connect(user=DB_USER,password=DB_PWD,database=DB, host='sitkaspar', ssl={'ssl': 1})
    # engine, suppose it has two tables 'user' and 'run' set up
    ssl_args = {'ssl_ca': '/etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt'}
    engine = create_engine(SQLALCHEMY_DATABASE_URI, connect_args=ssl_args)


    # reflect the tables
    Base.prepare(engine, reflect=True)
    #
    # # mapped classes are now created with names by default
    # # matching that of the table name.
    # # User = Base.classes.user
    Run = Base.classes.run
    # print(dir(Base.classes))
    IlluminaSequencingStats = Base.classes.illumina_sequencing_stats
    #
    session = Session(engine)
    link_results(lims,run_id)
