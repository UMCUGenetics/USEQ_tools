from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from xml.dom.minidom import parse
from config import Config
from genologics.entities import Project
from pathlib import Path
import os
import glob
import sys
import xml.etree.cElementTree as ET
import csv
import json
import shutil
from datetime import datetime
def parseConversionStatsType1( conversion_stats_xml_file ):
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


    for sample in getelements(conversion_stats_xml_file, "Sample"):
        if sample.attrib['name'] == 'all':continue
        sample_yield1 = 0
        sample_yield2 = 0
        sample_stats = {
            'SampleID' : sample.attrib['name'],
            'Index' : sample.find("./Barcode").attrib['name'],
            '# Reads' : 0,
            'Read 1 Mean Quality Score (PF)' : 0,
            'Read 2 Mean Quality Score (PF)' : 0,
            'Read 1 % Q30': 0,
            'Read 2 % Q30': 0,
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
        rows = 0
        with open(qual_metrics,'r') as q:

            csv_reader = csv.DictReader(q)
            for row in csv_reader:
                rows +=1
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
                stats['total_q30'] += float(row['% Q30'])
                stats['total_mean_qual'] += float(row['Mean Quality Score (PF)'])

        stats['total_q30'] = (stats['total_q30']/rows) * 100
        stats['total_mean_qual'] = stats['total_mean_qual'] / rows
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
        rows = 0
        for row in csv_reader:
            rows +=1
            stats['total_reads'] += float(row['# Reads'])

            samples_tmp[ row['SampleID'] ]['Index'] = row['Index']
            samples_tmp[ row['SampleID'] ]['Lane'] = int(row['Lane'])
            samples_tmp[ row['SampleID'] ]['# Reads'] += int(row['# Reads'])
            samples_tmp[ row['SampleID'] ]['# Perfect Index Reads']  += int(row['# Perfect Index Reads'])
            samples_tmp[ row['SampleID'] ]['# One Mismatch Index Reads']  += int(row['# One Mismatch Index Reads'])
            samples_tmp[ row['SampleID'] ]['Read 1 Mean Quality Score (PF)'] += float(row['Mean Quality Score (PF)'])
            samples_tmp[ row['SampleID'] ]['Read 1 % Q30'] += int(row['# of >= Q30 Bases (PF)'])
            stats['total_q30'] += int(row['# of >= Q30 Bases (PF)'])
            stats['total_mean_qual'] += float(row['Mean Quality Score (PF)'])
            if samples_tmp[ row['SampleID'] ]['Read 2 Bases'] > 0:
                samples_tmp[ row['SampleID'] ]['Read 2 Mean Quality Score (PF)'] += float(row['Mean Quality Score (PF)'])
                samples_tmp[ row['SampleID'] ]['Read 2 % Q30'] += int(row['# of >= Q30 Bases (PF)'])

        stats['total_q30'] = stats['total_q30']/rows
        stats['total_mean_qual'] = stats['total_mean_qual']/rows

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

def getNanoporeStats():
    #Gather nanopore runs
    nanopore_stats_dir = Path(f"{Config.HPC_STATS_DIR}/nanopore")
    stats = {}
    for summary in glob.glob(f"{nanopore_stats_dir}/**/*.txt", recursive=True):
        parent_dir = Path(summary).parent
        with open(summary, 'r') as s:
            tmp = {}
            for line in s.readlines():
                name,val = line.rstrip().split("=")
                tmp[name] = val
            if 'protocol_group_id' in tmp and 'flow_cell_id' in tmp:
                stats_pdf_search = glob.glob(f"{parent_dir}/*pdf")
                stats_html_search = glob.glob(f"{parent_dir}/*html")
                if tmp['protocol_group_id'] not in stats:
                    stats[ tmp['protocol_group_id'] ] = []
                if stats_pdf_search:
                    stats[ tmp['protocol_group_id'] ].append( {
                        'flowcell_id' : tmp['flow_cell_id'],
                        'stats_file' : stats_pdf_search[0],
                        'date' : tmp['started'].split("T")[0]
                    })
                elif stats_html_search:
                    runs[ run_date ] = {
                        'flowcell_id' : tmp['flow_cell_id'],
                        'stats_file' : stats_html_search[0],
                        'date' : run_date
                    }
                else:
                    stats[ tmp['protocol_group_id'] ].append( {
                        'flowcell_id' : tmp['flow_cell_id'],
                        'stats_file' : None,
                        'date' : tmp['started'].split("T")[0]
                    })

    return stats

def getIlluminaSequencingResults( lims, project):
    """Get the most recent raw run info based on project name and allowed RUN_PROCESSES"""
    runs = []

    project_processes = lims.get_processes(
        projectname=project.name,
        type=Config.RUN_PROCESSES
    )

    for process in project_processes:
        run_id = None
        flowcell_id = None
        if 'Run ID' in process.udf: run_id = process.udf['Run ID']

        if 'Flow Cell ID' in process.udf: flowcell_id = process.udf['Flow Cell ID']
        # print(process)
        runs.append({
            'date_run' : process.date_run,
            'run_id' : run_id,
            'flowcell_id' : flowcell_id,
            'run_dir' : None
        })

    if not runs:
        return None

    # print('Run',runs)

    for run in runs:
        # print(run)
        stats_dir = Path(Config.HPC_STATS_DIR)
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
    nanopore_stats = getNanoporeStats()
    # print(run_id)
    if run_id:
        runs = session.query(Run).filter_by(run_id=run_id).all()
    else:
        runs = session.query(Run).all()

    for run in runs[::-1]:
        print(run.run_id)
        project = None
        try:
            project = Project(lims, id=run.run_id)
            project_name = project.name
        except:
            print (f"Error : Project ID {run_id} not found!")
            continue


        ill_seq_results = getIlluminaSequencingResults(lims, project)
        nan_seq_results = nanopore_stats.get(project.id, None)



        if nan_seq_results:
            for seq_result in nan_seq_results:
                if not seq_result['flowcell_id']: continue

                prev_results = session.query(NanoporeSequencingStats).filter_by(flowcell_id=seq_result['flowcell_id']).first()
                if prev_results:continue
                nan_stats = None
                if seq_result['stats_file']:
                    tmp_stats_dir = Path(f"{Config.HPC_TMP_DIR}/{seq_result['flowcell_id']}")
                    if not tmp_stats_dir.is_dir():
                        tmp_stats_dir.mkdir()

                    os.system(f"scp {seq_result['stats_file']} {tmp_stats_dir}")
                    # os.system(f"rsync -r {tmp_stats_dir.as_posix()} {PORTAL_STORAGE}")

                    nan_stats = NanoporeSequencingStats(
                        general_stats = Path(seq_result['stats_file']).name,
                        date = seq_result['date'],
                        flowcell_id = seq_result['flowcell_id'],
                        run_id=run.id
                    )
                    print(seq_result['stats_file'],seq_result['date'],seq_result['flowcell_id'],run.id)
                else:
                    nan_stats = NanoporeSequencingStats(
                        date = seq_result['date'],
                        flowcell_id = seq_result['flowcell_id'],
                        run_id=run.id
                    )
                    print('NA',seq_result['date'],seq_result['flowcell_id'],run.id)
                session.add(nan_stats)
                session.commit()

        elif ill_seq_results:
            for seq_result in ill_seq_results:
                if not seq_result['flowcell_id']: continue

                prev_results = session.query(IlluminaSequencingStats).filter_by(flowcell_id=seq_result['flowcell_id']).first()
                if prev_results:continue
                tmp_stats_dir = Path(f"{Config.HPC_TMP_DIR}/{seq_result['flowcell_id']}")
                conversion_stats_json_file = Path(f"{tmp_stats_dir}/Conversion_Stats.json")
                if not tmp_stats_dir.is_dir():
                    tmp_stats_dir.mkdir()

                ill_stats = None
                if seq_result['run_dir']:
                    run_name = Path(seq_result['run_dir']).name

                    conversion_stats_xml_file = Path(f"{seq_result['run_dir']}/ConversionStats.xml")

                    demux_stats_file = Path(f"{seq_result['run_dir']}/Demultiplex_Stats.csv")
                    qual_metrics_file = Path(f"{seq_result['run_dir']}/Quality_Metrics.csv")
                    adapter_metrics_file = Path(f"{seq_result['run_dir']}/Adapter_Metrics.csv")
                    summary_stats_file = Path(f"{seq_result['run_dir']}/{ Path(seq_result['run_dir']).name }_summary.csv")
                    stats = None
                    if conversion_stats_xml_file.is_file():
                        stats = parseConversionStatsType1(conversion_stats_xml_file) #bcl2fastq conversion

                        # print('opt1', stats)
                    elif qual_metrics_file.is_file():
                        stats = parseConversionStatsType2(lims, run.run_id, qual_metrics_file,demux_stats_file) #bcl-convert >= 3.9
                        # print('opt2', stats)
                    elif adapter_metrics_file.is_file():
                        stats = parseConversionStatsType3(lims, run.run_id, adapter_metrics_file,demux_stats_file) #bcl-convert < 3.9
                    else:
                        continue

                    with open (conversion_stats_json_file, 'w') as c:
                        c.write(json.dumps(stats))



                    for machine in Config.MACHINE_ALIASES:

                        possible_img_dir = Path(f'{Config.HPC_RAW_ROOT}/{machine}/{run_name}')

                        if possible_img_dir.is_dir():
                            base = None
                            if Path(f'{dir}/{run_name}/Data/Intensities/BaseCalls/Stats/{run_name}_summary.csv').is_file():
                                print(f'OLD {dir}/{run_name}/Data/Intensities/BaseCalls/Stats/{run_name}_summary.csv')
                                base = f'{dir}/{run_name}/Data/Intensities/BaseCalls/Stats/{run_name}'

                            elif Path(f'{dir}/{run_name}/Conversion/Reports/{run_name}_summary.csv').is_file():
                                print(f'{dir}/{run_name}/Conversion/Reports/{run_name}_summary.csv')
                                base = f'{dir}/{run_name}/Conversion/Reports/{run_name}'

                            if base:
                                flowcell_intensity_plot = f'{base}_flowcell-Intensity.png'
                                flowcell_density_plot = f'{base}_Clusters-by-lane.png'
                                total_qscore_lanes_plot = f'{base}_q-histogram.png'
                                cycle_qscore_lanes_plot = f'{base}_q-heat-map.png'
                                cycle_base_plot = f'{base}_BasePercent-by-cycle_BasePercent.png'
                                cycle_intensity_plot = f'{base}_Intensity-by-cycle_Intensity.png'
                                print(base, seq_result['flowcell_id'])
                                # print(flowcell_intensity_plot)
                                # os.system(f"scp {flowcell_intensity_plot} {TMP_DIR}/{seq_result['flowcell_id']}")
                                shutil.copy(flowcell_intensity_plot,tmp_stats_dir.as_posix())
                                shutil.copy(flowcell_density_plot,tmp_stats_dir.as_posix())
                                shutil.copy(total_qscore_lanes_plot,tmp_stats_dir.as_posix())
                                shutil.copy(cycle_qscore_lanes_plot,tmp_stats_dir.as_posix())
                                shutil.copy(cycle_base_plot,tmp_stats_dir.as_posix())
                                shutil.copy(cycle_intensity_plot,tmp_stats_dir.as_posix())

                                # os.system(f"rsync -r {tmp_stats_dir.as_posix()} {PORTAL_STORAGE}")

                                ill_stats = IlluminaSequencingStats(
                                    flowcell_id=seq_result['flowcell_id'],
                                    general_stats="Conversion_Stats.json",
                                    date = seq_result['date_run'],
                                    flowcell_intensity_plot = f'{run_name}_flowcell-Intensity.png',
                                    flowcell_density_plot = f'{run_name}_Clusters-by-lane.png',
                                    total_qscore_lanes_plot = f'{run_name}_q-histogram.png',
                                    cycle_qscore_lanes_plot = f'{run_name}_q-heat-map.png',
                                    cycle_base_plot = f'{run_name}_BasePercent-by-cycle_BasePercent.png',
                                    cycle_intensity_plot = f'{run_name}_Intensity-by-cycle_Intensity.png',
                                    run_id=run.id
                                )



                # os.system(f"rsync -r {tmp_stats_dir.as_posix()} {PORTAL_STORAGE}")
                if not ill_stats:
                    if conversion_stats_json_file.is_file():
                        ill_stats = IlluminaSequencingStats(
                            flowcell_id=seq_result['flowcell_id'],
                            general_stats="Conversion_Stats.json",
                            date = seq_result['date_run'],
                            run_id=run.id
                        )
                        print('No plots')
                    else:
                        ill_stats = IlluminaSequencingStats(
                            flowcell_id=seq_result['flowcell_id'],
                            date = seq_result['date_run'],
                            run_id=run.id
                        )
                        print('No stats')
                else:
                    print('Stats')
                session.add(ill_stats)
                session.commit()
def run(lims, run_id):

    global session
    # global User
    global Run
    global IlluminaSequencingStats
    global NanoporeSequencingStats
    Base = automap_base()
# pymysql.connect(user=DB_USER,password=DB_PWD,database=DB, host='sitkaspar', ssl={'ssl': 1})
    # engine, suppose it has two tables 'user' and 'run' set up
    ssl_args = {'ssl_ca': '/etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt'}
    engine = create_engine(Config.PORTAL_DB_URI, connect_args=ssl_args)


    # reflect the tables
    Base.prepare(engine, reflect=True)
    #
    # # mapped classes are now created with names by default
    # # matching that of the table name.
    # # User = Base.classes.user
    Run = Base.classes.run
    # print(dir(Base.classes))
    IlluminaSequencingStats = Base.classes.illumina_sequencing_stats
    NanoporeSequencingStats = Base.classes.nanopore_sequencing_stats

    session = Session(engine)
    link_results(lims,run_id)
