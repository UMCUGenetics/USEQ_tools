import logging
import subprocess
import xml.dom.minidom
import shlex
import sys
import traceback
import csv
import json
import os
import time
import shutil
import re
import time
from genologics.entities import Project
from itertools import islice
from pathlib import Path
from modules.useq_illumina_parsers import getExpectedReads,parseSampleSheet
from modules.useq_nextcloud import NextcloudUtil
from modules.useq_template import TEMPLATE_PATH,TEMPLATE_ENVIRONMENT,renderTemplate
from modules.useq_mail import sendMail
from config import Config

def addFlowcellToFastq(project_directory, flowcell_id, logger):
    """Add flowcell id to fastq.gz filename."""
    logger.info('Adding flowcell to fastqs')

    for fastq in project_directory.rglob("*.fastq.gz"):
        filename_parts = fastq.name.split('_')
        if filename_parts[1] != flowcell_id:
            filename_parts.insert(1, flowcell_id)
            new_filename = '_'.join(filename_parts)
            fastq.rename(f'{fastq.parent}/{new_filename}')

def cleanup(run_dir, logger):
    logger.info('Deleting FastQ files')
    for file in run_dir.glob("**/*.gz"):
        if file.name.endswith(".fastq.gz") or file.name.endswith(".fq.gz"):
            file.unlink()

def revAndCleanSample(header, sample):

    sample[ header.index('index') ] = sample[header.index('index')].replace("N","")

    sample_rev = sample.copy()

    if 'index2' in header:

        revseq = revcomp(sample_rev[header.index('index2')])
        sample_rev[header.index('index2')] = revseq
    else:
        revseq = revcomp(sample_rev[header.index('index')])
        sample_rev[header.index('index')] = revseq

    return [sample, sample_rev]

def demuxCheckSamplesheet(sample_sheet,pid, project_data, run_data,first_tile ,logger):

    run_dir ="/"+"/".join(sample_sheet.parts[1:sample_sheet.parts.index('Conversion')])
    demux_out_dir = f"{sample_sheet.parent}/{sample_sheet.stem}"

    logger.info(f'Running demultiplexing check on {sample_sheet.name}')
    command = f'{Config.CONV_BCLCONVERT}/bcl-convert --bcl-input-directory {run_dir} --output-directory {demux_out_dir} --sample-sheet {sample_sheet} --bcl-sampleproject-subdirectories true --force --tiles {first_tile}'

    if not runSystemCommand(command, logger):
        logger.error(f'Failed to run demultiplexing check on {sample_sheet.name}')
        raise
    #
    logger.info(f'Checking demultiplexing stats for {demux_out_dir}/Reports')
    stats = parseConversionStats(f'{demux_out_dir}/Reports')

    if stats['undetermined_reads'] / stats['total_reads'] < 0.40:
        logger.info(f'Demultiplexing check stats for {sample_sheet.name} PASSED')
        return True
    else:

        # print(stats['total_reads_lane'])
        for lane in project_data['on_lanes']:
            total_reads = stats['total_reads']
            total_reads_lane = stats['total_reads_lane'][lane]
            nr_samples_lane = len(run_data['lanes'][lane]['samples'])
            nr_samples_project = len(project_data['samples'])
            avg_reads_per_sample = total_reads_lane / nr_samples_lane
            expected_project_reads = avg_reads_per_sample * nr_samples_project
            avg_reads_project_lane = stats['total_reads_project'][pid] / len( project_data['on_lanes'] )
            perc_diff = abs(expected_project_reads -avg_reads_project_lane ) / ((expected_project_reads + avg_reads_project_lane)/2)
            print(f"Lane {lane}")
            print(f"Nr samples lane                             {nr_samples_lane}")
            print(f"Total reads lane                            {total_reads_lane}")
            print(f"Avg reads per sample                        {avg_reads_per_sample}")
            print(f"Expected reads for project for lane {lane}  {expected_project_reads}")
            print(f"Avg reads for project for lane {lane}  {avg_reads_project_lane}")
            print(f"Diff between expected and avg reads {lane}  {perc_diff}")

            if perc_diff > 0.5:
                logger.info(f'Demultiplexing check stats for {sample_sheet.name} on lane {lane} and projectID {pid} FAILED')
                return False
            # if expected_project_reads > avg_reads_project_lane or

        # sys.exit()
        logger.info(f'Demultiplexing check stats for {sample_sheet.name} PASSED')
        return True

def demultiplexPID(run_dir, pid, project_data, run_data, master_sheet, first_tile ,logger):

    samples = project_data['samples']
    if len(samples) > 1: #More than 1 sample, check if demultiplexing will succeed
        project_directory = Path(f"{run_dir}/Conversion/{pid}/")
        project_directory.mkdir(parents=True, exist_ok=True)
        demux_directory = Path(f"{project_directory}/Demux-check")
        demux_directory.mkdir(parents=True, exist_ok=True)
        flowcell = run_dir.name.split("_")[-1]

        logger.info(f'Found more than 1 samples, moving on with demux test.')

        sample_sheet = Path(f'{demux_directory}/SampleSheet-{pid}.csv')
        sample_sheet_rev = Path(f'{demux_directory}/SampleSheet-{pid}-rev.csv')
        correct_samplesheet = None
        with open(sample_sheet, 'w') as ss, open(sample_sheet_rev, 'w') as ssr:
            logger.info(f'Creating samplesheet {sample_sheet}')
            logger.info(f'Creating rev samplesheet {sample_sheet_rev}')

            ss.write(master_sheet['top'])
            ss.write(f'{",".join( master_sheet["header"] )}\n')
            ssr.write(master_sheet['top'])
            ssr.write(f'{",".join( master_sheet["header"] )}\n')

            for sample in samples:
                sample_and_revsample = revAndCleanSample(master_sheet['header'],sample)
                ss.write(f'{",".join(sample_and_revsample[0])}\n')
                ssr.write(f'{",".join(sample_and_revsample[1])}\n')


        if demuxCheckSamplesheet(sample_sheet,pid,project_data, run_data ,first_tile, logger):
            correct_samplesheet = sample_sheet
        elif demuxCheckSamplesheet(sample_sheet_rev,pid,project_data, run_data , first_tile, logger):
            correct_samplesheet = sample_sheet_rev
        else:
            logger.error(f'Could not create a correct samplesheet for projectID {pid}, skipping demux.')
            return False

        # logger.info(f'Demux test for samplesheet {correct_samplesheet.name} PASSED')
        shutil.move(correct_samplesheet, f'{project_directory}/{correct_samplesheet.name}')
        correct_samplesheet = Path(f'{project_directory}/{correct_samplesheet.name}')

        logger.info('Starting demultiplexing')
        # command = f'{Config.CONV_BCLCONVERT}/bcl-convert --bcl-input-directory {run_dir} --output-directory {run_dir}/Conversion/FastQ --bcl-sampleproject-subdirectories true --force --sample-sheet {correct_samplesheet}'
        command = f'{Config.CONV_BCLCONVERT}/bcl-convert --bcl-input-directory {run_dir} --output-directory {project_directory} --force --sample-sheet {correct_samplesheet}'
        if Config.DEVMODE: command += f" --tiles {first_tile} "
        if not runSystemCommand(command, logger):
            logger.error(f'Failed to run demultiplexing for projectID {pid}.')
            raise

        addFlowcellToFastq(project_directory, flowcell, logger)

        return True


    else:
        logger.info(f'Found 1 sample, skipping demux.')
        return False

def filterStats(lims, pid, pid_staging, report_dir):
    samples = lims.get_samples(projectlimsid=pid)
    sample_names = [x.name for x in samples]
    adapter_metrics = Path(f'{report_dir}/Adapter_Metrics.csv')
    adapter_metrics_filtered = Path(f'{pid_staging}/Adapter_Metrics.csv')
    demultiplex_stats = Path(f'{report_dir}/Demultiplex_Stats.csv')
    demultiplex_stats_filtered = Path(f'{pid_staging}/Demultiplex_Stats.csv')
    if adapter_metrics.is_file():
        with open(adapter_metrics, 'r') as original, open(adapter_metrics_filtered, 'w') as filtered:
            for line in original.readlines():
                parts = line.split(',')
                if line.startswith('Lane') or parts[1] in sample_names:
                    filtered.write(line)
    if demultiplex_stats.is_file():
        with open(demultiplex_stats, 'r') as original, open(demultiplex_stats_filtered, 'w') as filtered:
            for line in original.readlines():
                parts = line.split(',')
                if line.startswith('Lane') or parts[1] in sample_names:
                    filtered.write(line)


def generateRunStats(run_dir, logger):
    """Create run stats files using interop tool."""
    conversion_dir = Path(f'{run_dir}/Conversion/')
    stats_dir = Path(f'{run_dir}/Conversion/Reports')
    fastqc_dir = Path(f'{run_dir}/Conversion/Reports/fastqc')
    multiqc_dir = Path(f'{run_dir}/Conversion/Reports/multiqc')

    if not fastqc_dir.is_dir():
        fastqc_dir.mkdir(parents=True, exist_ok=True)
    os.chdir(stats_dir)

    try:

        logger.info('Generating run summary')
        summary_csv = open(f'{stats_dir}/{run_dir.name}_summary.csv', 'w')
        subprocess.run([f'{Config.CONV_INTEROP}/bin/summary',run_dir], stdout=summary_csv, check=True, stderr=subprocess.PIPE)

        logger.info('Generating run plots')
        p1 = subprocess.Popen([f'{Config.CONV_INTEROP}/bin/plot_by_cycle',run_dir,'--metric-name=Intensity'], stdout=subprocess.PIPE)
        p2 = subprocess.run(['gnuplot'], stdin=p1.stdout, check=True, stderr=subprocess.PIPE)

        p1 = subprocess.Popen([f'{Config.CONV_INTEROP}/bin/plot_by_cycle',run_dir,'--metric-name=BasePercent'], stdout=subprocess.PIPE)
        p2 = subprocess.run(['gnuplot'], stdin=p1.stdout, check=True, stderr=subprocess.PIPE)

        p1 = subprocess.Popen([f'{Config.CONV_INTEROP}/bin/plot_by_lane',run_dir,'--metric-name=Clusters'], stdout=subprocess.PIPE)
        p2 = subprocess.run(['gnuplot'], stdin=p1.stdout, check=True, stderr=subprocess.PIPE)

        p1 = subprocess.Popen([f'{Config.CONV_INTEROP}/bin/plot_flowcell',run_dir], stdout=subprocess.PIPE)
        p2 = subprocess.run(['gnuplot'], stdin=p1.stdout, check=True, stderr=subprocess.PIPE)

        p1 = subprocess.Popen([f'{Config.CONV_INTEROP}/bin/plot_qscore_heatmap',run_dir], stdout=subprocess.PIPE)
        p2 = subprocess.run(['gnuplot'], stdin=p1.stdout, check=True, stderr=subprocess.PIPE)

        p1 = subprocess.Popen([f'{Config.CONV_INTEROP}/bin/plot_qscore_histogram',run_dir], stdout=subprocess.PIPE)
        p2 = subprocess.run(['gnuplot'], stdin=p1.stdout, check=True, stderr=subprocess.PIPE)

        if not Config.DEVMODE:
            logger.info('Running FastQC')
            fastqc_command = f'{Config.CONV_FASTQC}/fastqc -t 24 -q {run_dir}/Conversion/**/*_R*fastq.gz -o {fastqc_dir}'
            if not runSystemCommand(fastqc_command, logger, shell=True):
                logger.error(f'Failed to run FastQC.')
                raise

            logger.info('Running MultiQC')
            multiqc_command = f'multiqc {fastqc_dir} -o {multiqc_dir} -n {run_dir.name}_multiqc_report.html'
            if not runSystemCommand(multiqc_command, logger, shell=True):
                logger.error(f'Failed to run MultiQC.')
                raise


    except subprocess.CalledProcessError as e:
        logger.error(
            f'Failed to generate run stats'
            f'Returned {e.returncode}\n{e}\n'
            f'{e.stderr}\n'
        )

    with open(f'{stats_dir}/Demultiplex_Stats.csv', 'w') as all_demux_stats:
        first_line = None
        for demultiplex_stats in conversion_dir.glob('*/Reports/Demultiplex_Stats.csv'):
            project_id = demultiplex_stats.parts[-3]
            with open(demultiplex_stats, 'r') as ds:
                if not first_line:
                    first_line = ds.readline().split(",")
                    first_line.insert(2,'Sample_Project')
                    all_demux_stats.write(",".join(first_line))
                else:
                    ds.readline()
                for line in ds.readlines():
                    line_parts = line.split(",")
                    line_parts.insert(2,project_id)
                    all_demux_stats.write(",".join(line_parts))

                # all_demux_stats.write(ds.read())

    with open(f'{stats_dir}/Quality_Metrics.csv', 'w') as all_qm_stats:
        first_line = None
        for qm_stats in conversion_dir.glob('*/Reports/Quality_Metrics.csv'):
            project_id = qm_stats.parts[-3]
            with open(qm_stats, 'r') as qs:
                if not first_line:
                    first_line = qs.readline().split(",")
                    first_line.insert(2,'Sample_Project')
                    all_qm_stats.write(",".join(first_line))
                else:
                    x = qs.readline()
                for line in qs.readlines():
                    line_parts = line.split(",")
                    line_parts.insert(2,project_id)
                    all_qm_stats.write(",".join(line_parts))

    with open(f'{stats_dir}/Top_Unknown_Barcodes.csv', 'w') as all_top_unknown:
        first_line = None
        for tu_stats in conversion_dir.glob('*/Reports/Top_Unknown_Barcodes.csv'):
            with open(tu_stats, 'r') as tu:
                if not first_line:
                    first_line = tu.readline()
                    all_top_unknown.write(first_line)
                else:
                    x = tu.readline()

                all_top_unknown.write(tu.read())

    return True

def getExpectedYield(run_info_xml, expected_reads):
    run_info = xml.dom.minidom.parse(run_info_xml)

    yields = { 'r1':0,'r2':0 }

    for read in run_info.getElementsByTagName('Read'):
        if read.getAttribute('IsIndexedRead') == 'N':
            if int(read.getAttribute('Number')) == 1:
                yields['r1'] = (float( read.getAttribute('NumCycles')) * expected_reads) / 1000000000
            else:
                yields['r2'] = (float( read.getAttribute('NumCycles')) * expected_reads) / 1000000000
    return yields



def parseConversionStats(dir):
    demux_stats = f'{dir}/Demultiplex_Stats.csv'
    qual_metrics = Path(f'{dir}/Quality_Metrics.csv')
    top_unknown = f'{dir}/Top_Unknown_Barcodes.csv'
    stats = {
        'total_reads' : 0,
        'total_reads_lane' : {},
        'total_reads_project' : {},
        'total_reads_lane_project' : {},
        'undetermined_reads' : 0,
        'samples' : {},
        'top_unknown' : {}
    }
    samples_tmp = {}
    with open(demux_stats, 'r') as d:
        csv_reader = csv.DictReader(d)
        for row in csv_reader:
            sample_id = row['SampleID']
            lane = int(row['Lane'])
            project_id = row['Sample_Project']
            if lane not in samples_tmp:
                samples_tmp[ lane ] = {}

            if sample_id not in samples_tmp[ lane ]:
                samples_tmp[ lane ][ sample_id ] = {
                    'ProjectID' : project_id,
                    'Index' : None,
                    '# Reads' : 0,
                    '# Perfect Index Reads' : 0,
                    '# One Mismatch Index Reads' : 0,
                }
            if project_id not in stats['total_reads_project']:
                stats['total_reads_project'][project_id] = 0

            if lane not in stats['total_reads_lane_project']:
                stats['total_reads_lane_project'][lane] = {}
            if project_id not in stats['total_reads_lane_project'][lane]:
                stats['total_reads_lane_project'][lane][project_id] = 0

            if sample_id == 'Undetermined':
                stats['undetermined_reads'] += float(row['# Reads'])
            else:
                stats['total_reads_project'][project_id] += float(row['# Reads'])
                stats['total_reads_lane_project'][lane][project_id]+= float(row['# Reads'])

            stats['total_reads'] += float(row['# Reads'])

            if lane not in stats['total_reads_lane']:
                stats['total_reads_lane'][lane] = 0
            stats['total_reads_lane'][lane] += float(row['# Reads'])

            samples_tmp[ lane ][ sample_id ]['Index'] = row['Index']
            samples_tmp[ lane ][ sample_id ]['# Reads'] += int(row['# Reads'])
            samples_tmp[ lane ][ sample_id ]['# Perfect Index Reads']  += int(row['# Perfect Index Reads'])
            samples_tmp[ lane ][ sample_id ]['# One Mismatch Index Reads']  += int(row['# One Mismatch Index Reads'])

    if qual_metrics.is_file():
        with open(qual_metrics,'r') as q:
            csv_reader = csv.DictReader(q)
            for row in csv_reader:
                sample_id = row['SampleID']
                lane = int(row['Lane'])
                mqs = f'Read {row["ReadNumber"]} Mean Quality Score (PF)'
                q30 = f'Read {row["ReadNumber"]} % Q30'
                if mqs not in samples_tmp[ lane ][ sample_id ]:
                    samples_tmp[ lane ][ sample_id ][mqs] = 0
                if q30 not in samples_tmp[ lane ][ sample_id ]:
                    samples_tmp[ lane ][ sample_id ][q30] = 0

                samples_tmp[ lane ][ sample_id ][mqs] += float(row['Mean Quality Score (PF)'])
                samples_tmp[ lane ][ sample_id ][q30] += float(row['% Q30'])

    for lane in samples_tmp:
        for sample_id in samples_tmp[ lane ]:
            if lane not in stats['samples']:
                stats['samples'][ lane ] = {}
            if sample_id not in stats['samples'][ lane ]:
                stats['samples'][ lane ][ sample_id ] = {}

            # sample = {}
            for read_number in ['1','2','I1','I2']:
                if f'Read {read_number} Mean Quality Score (PF)' in samples_tmp[ lane ][sample_id]:
                    stats['samples'][ lane ][ sample_id ][f'Read {read_number} Mean Quality Score (PF)'] = samples_tmp[ lane ][sample_id][f'Read {read_number} Mean Quality Score (PF)']
                if f'Read {read_number} % Q30' in samples_tmp[ lane ][sample_id]:
                    stats['samples'][ lane ][ sample_id ][f'Read {read_number} % Q30'] = samples_tmp[ lane ][sample_id][f'Read {read_number} % Q30'] * 100
            stats['samples'][ lane ][ sample_id ]['SampleID'] = sample_id
            stats['samples'][ lane ][ sample_id ]['ProjectID'] = samples_tmp[ lane ][sample_id]['ProjectID']
            stats['samples'][ lane ][ sample_id ]['Index'] = samples_tmp[ lane ][sample_id]['Index']
            stats['samples'][ lane ][ sample_id ]['# Reads'] = samples_tmp[ lane ][sample_id]['# Reads']
            stats['samples'][ lane ][ sample_id ]['# Perfect Index Reads'] = samples_tmp[ lane ][sample_id]['# Perfect Index Reads']
            stats['samples'][ lane ][ sample_id ]['# One Mismatch Index Reads'] = samples_tmp[ lane ][sample_id]['# One Mismatch Index Reads']
            # stats['samples'].append(sample)

    with open(top_unknown, 'r') as t:
        csv_reader = csv.DictReader(t)
        # for row in islice(csv_reader,0,20):
        for row in csv_reader:
            lane = int(row['Lane'])
            if lane not in stats['top_unknown']:
                stats['top_unknown'][lane] = []
            if len(stats['top_unknown'][lane]) < 5:
                stats['top_unknown'][ lane ].append(row)
    return stats

def parseSummaryStats( summary ):
    stats = {
        'summary' : [],
        'all' : {}
    }
    tmp = {}
    with open(summary, 'r') as sumcsv:
        lines = sumcsv.readlines()
        line_nr = 0
        while line_nr < len(lines):
            line = lines[line_nr].rstrip()
            if not line: line_nr+=1;continue
            if line.startswith('Level'):
                header = [x.rstrip() for x in line.split(",")]
                sub_lines = lines[line_nr+1:]
                for sub_line in sub_lines:
                    line_nr +=1
                    cols = [x.rstrip() for x in sub_line.split(",")]
                    stats['summary'].append(dict(zip(header,cols)))
                    if sub_line.startswith('Total'): break
            elif line.startswith('Read'):
                read = line.rstrip()
                line_nr += 1
                header_line = lines[line_nr].rstrip()
                header = [x.rstrip() for x in header_line.split(",")]
                sub_lines = lines[line_nr+1:]
                # print("READ", read)
                # print("HEADER",header_line)
                if read not in tmp:
                    tmp[read] = {}
                for sub_line in sub_lines:
                    line_nr+=1
                    if sub_line.startswith('Read') or sub_line.startswith('Extracted'): break
                    cols = [x.rstrip().split(" ")[0] for x in sub_line.split(",")]
                    if cols[1] == "-": #lane summary line
                        col_dict = dict(zip(header,cols))
                        # print("DATA",col_dict['Lane'], col_dict)
                        tmp[read][ col_dict['Lane'] ] = col_dict
            else:
                line_nr += 1

    for read in tmp:
        if '(I)' in read:continue
        for lane in tmp[read]:
            if lane not in stats['all']:
                stats['all'][lane] = {}
            if read not in stats['all'][lane]:
                stats['all'][lane][read] = []
            stats['all'][lane][read] = tmp[read][lane]
    return stats


def revcomp(seq):
    revcompl = lambda x: ''.join([{'A':'T','C':'G','G':'C','T':'A'}[B] for B in x][::-1])
    return revcompl(seq)

def runSystemCommand(command, logger, shell=False):
    #split command into pieces for subprocess.run
    command_pieces = shlex.split(command)
    # print(command_pieces)
    logger.info(f'Running command : {command}')

    try:
        if shell:
            subprocess.run(command, check=True, shell=shell,stderr=subprocess.PIPE)
        else:
            subprocess.run(command_pieces, check=True, shell=shell,stderr=subprocess.PIPE)
    except FileNotFoundError as e:
        logger.error(f'Process failed because the executable could not be found\n{e}')

        return False
    except subprocess.CalledProcessError as e:

        logger.error(
            f'Process failed because did not return a successful return code\n'
            f'Returned {e.returncode}\n{e}\n'
            f'{e.stderr}\n'
        )
        return False
    else:
        return True



def updateStatus(file, status):

    # status[step] = bool
    with open(file, 'w') as f:
        f.write(json.dumps(status, indent=4))

def uploadToArchive(run_dir, logger):
    machine = None
    if 'MyRun' in run_dir.parents[0].name:
        machine = run_dir.parents[1].name
    else:
        machine = run_dir.parents[0].name
    command = "rsync -rahm "
    if Config.DEVMODE:
        command += "--dry-run "

    logger.info('Uploading run folder to archive storage')

    if machine == 'WES-WGS':
        command += f"--exclude '*jpg' {run_dir} {Config.USEQ_USER}@{Config.HPC_TRANSFER_SERVER}:{Config.HPC_ARCHIVE_DIR}/{machine}"
    else:
        command += f"--exclude '*jpg' --exclude '*fastq.gz' --exclude '*fq.gz' {run_dir} {Config.USEQ_USER}@{Config.HPC_TRANSFER_SERVER}:{Config.HPC_ARCHIVE_DIR}/{machine}"

    if not runSystemCommand(command, logger):
        logger.error(f'Failed upload run folder to archive storage')
        raise

    return True

def uploadToHPC(lims, run_dir, pid, logger):
    machine = None
    if 'MyRun' in run_dir.parents[0].name:
        machine = run_dir.parents[1].name
    else:
        machine = run_dir.parents[0].name
    to_sync = ''
    command = '/usr/bin/rsync -rah --update --stats --verbose --prune-empty-dirs '
    if Config.DEVMODE:
        command += '--dry-run '

    project = Project(lims, id=pid)
    project_name = project.name

    samples = lims.get_samples(projectlimsid=project.id)
    analysis_steps = samples[0].udf.get('Analysis','').split(',')
    if len(analysis_steps) > 1 or project.udf.get('Application','') == 'SNP Fingerprinting':
        command += f'--include "Conversion/{pid}/*.fastq.gz" '
    else:
        command += f'--exclude "Conversion/{pid}/*.fastq.gz" '

    command += f" --include '*/' --include 'md5sum.txt' --include 'SampleSheet.csv' --include 'RunInfo.xml' --include '*unParameters.xml' --include 'InterOp/**' --include '*/Conversion/{pid}/Reports/**' --include 'Data/Intensities/BaseCalls/Stats/**' --include '*.[pP][eE][dD]'"
    command += " --exclude '*'"
    command += f" {run_dir}"
    command += f" {Config.USEQ_USER}@{Config.HPC_TRANSFER_SERVER}:/{Config.HPC_RAW_ROOT}/{machine}"

    logger.info('Uploading run folder to HPC')

    if not runSystemCommand(command, logger):
        logger.error(f'Failed upload run folder to HPC')
        raise

    return True

def uploadToNextcloud(lims, run_dir, pid,logger,mode='fastq', skip_undetermined=False, lanes=None ):
    machine = None
    if 'MyRun' in run_dir.parents[0].name:
        machine = run_dir.parents[1].name
    else:
        machine = run_dir.parents[0].name
    flowcell = run_dir.name.split("_")[-1]
    #Create .tar files for upload to nextcloud
    # if mode == 'fastq' or mode == 'wgs':
    if mode == 'fastq':
        pid_staging = Path(f'{Config.CONV_STAGING_DIR}/{pid}')
        pid_staging.mkdir(parents=True, exist_ok=True)

        pid_samples  = set()
        pid_dir = Path(f'{run_dir}/Conversion/{pid}')
        for fastq in pid_dir.glob('*.fastq.gz'):
            name = fastq.name.split('_')[0]
            pid_samples.add(name)


        for sample in pid_samples:
            logger.info(f'Zipping samples for {pid}')
            sample_zip = Path(f'{pid_staging}/{sample}.tar')
            sample_zip_done = Path(f'{pid_staging}/{sample}.tar.done')
            if not sample_zip_done.is_file():
                os.chdir(pid_dir)
                command = f'tar -cvf {sample_zip} {sample}_*fastq.gz'
                if not runSystemCommand(command, logger, shell=True):
                    logger.error(f'Failed to create {sample_zip}')
                    raise
                else:
                    sample_zip_done.touch()

        # if mode != 'wgs':
        if skip_undetermined:
            logging.info(f'Zipping undetermined reads')
            und_zip = Path(f'{pid_staging}/undetermined.tar')
            und_zip_done = Path(f'{pid_staging}/Undetermined.tar.done')
            if not und_zip_done.is_file():
                os.chdir(pid_dir)
                command = f'tar -cvf {und_zip} Undetermined_*fastq.gz'
                if not runSystemCommand(command, logger, shell=True):
                    logger.error(f'Failed to create {und_zip}')
                    raise
                else:
                    und_zip_done.touch()

        logger.info(f'Filtering stats for {pid}')
        report_dir = Path(f'{run_dir}/Conversion/{pid}/Reports')
        filterStats(lims, pid, pid_staging, report_dir)

    else: #Upload BCL only
        pid_staging = Path(f'{Config.CONV_STAGING_DIR}/{pid}')
        pid_staging.mkdir(parents=True, exist_ok=True)


        zipped_run = Path(f'{pid_staging}/{pid}.tar')
        zip_done = Path(f'{pid_staging}/{pid}.tar.done')

        if not zip_done.is_file():
            logger.info(f'Zipping {run_dir.name} to {zipped_run}')
            os.chdir(run_dir.parents[0])
            # command += " --include '*/' --include 'md5sum.txt' --include 'SampleSheet.csv' --include 'RunInfo.xml' --include '*unParameters.xml' --include 'InterOp/**' --include '*/Conversion/Reports/**' --include '*/FastQ/Reports/**' --include 'Data/Intensities/BaseCalls/Stats/**' --include '*.[pP][eE][dD]'"
            tar_exclude = '--exclude "Data/" --exclude "*Conversion*" --exclude "*fastq.gz*" --exclude "*run_zip.*" --exclude "SampleSheet*" --exclude "status.json" --exclude "mgr.log" --exclude ".mgr_*" '

            tar_include = f'{run_dir.name}/Data/Intensities/s.locs '
            if lanes:
                for lane in lanes:
                    if Config.DEVMODE:
                        tar_include += f'{run_dir.name}/Data/Intensities/BaseCalls/L00{lane}/C1.1/ '
                    else:
                        tar_include += f'{run_dir.name}/Data/Intensities/BaseCalls/L00{lane}/ '
            else:
                tar_include += f'{run_dir.name}/Data/Intensities/BaseCalls/'
            command = f'tar -cvf {zipped_run} {tar_exclude} {tar_include} {run_dir.name}'

            if not runSystemCommand(command, logger, shell=True):
                logger.error(f'Failed to zip {run_dir.name} to {zipped_run}')
                raise
            else:
                zip_done.touch()


    #Upload .tar/stats & md5sums to nextcloud
    upload_id = f"{pid}_{flowcell}"
    pid_staging = Path(f'{Config.CONV_STAGING_DIR}/{pid}')
    transfer_done = Path(f'{Config.CONV_STAGING_DIR}/{upload_id}.done')


    if nextcloud_util.checkExists(upload_id) and nextcloud_util.checkExists(f'{upload_id}.done'):
        #Previous upload succeeded but needs to be replaced (e.g. conversion incorrect)
        logger.info(f'Deleting previous version of {upload_id} on Nextcloud')
        nextcloud_util.delete(upload_id)
        nextcloud_util.delete(f'{upload_id}.done')
    elif nextcloud_util.checkExists(upload_id):
        #Previous upload failed and needs to be replaced. First make sure a .done file get's uploaded, wait 1 minute for the user rights to change & delete it
        logger.info(f'Deleting previous version of {upload_id} on Nextcloud')
        transfer_done.touch()

        command = f"scp -r {transfer_done} {Config.NEXTCLOUD_HOST}:{Config.NEXTCLOUD_DATA_ROOT}/{Config.NEXTCLOUD_RAW_DIR}/"
        logger.info(f'Transferring {transfer_done} to nextcloud')
        if not runSystemCommand(command, logger):
            logger.error(f'Failed to upload {transfer_done} to nextcloud')
            raise
        transfer_done.unlink()
        time.sleep(60)
        nextcloud_util.delete(upload_id)
        nextcloud_util.delete(f'{upload_id}.done')

    tmp_dir = Path(f'{run_dir}/{upload_id}')
    tmp_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f'Creating nextcloud dir for {upload_id}')
    command = f"scp -r {tmp_dir} {Config.NEXTCLOUD_HOST}:{Config.NEXTCLOUD_DATA_ROOT}/{Config.NEXTCLOUD_RAW_DIR}/"
    if not runSystemCommand(command, logger):
        logger.error(f'Failed to create nextcloud dir for {upload_id}')
        tmp_dir.rmdir()
        raise

    tmp_dir.rmdir()


    logger.info(f'Creating md5sums for {upload_id}')
    os.chdir(pid_staging)
    command = f'md5sum *.tar > {pid_staging}/md5sums.txt'
    if not runSystemCommand(command, logger, shell=True):
        logger.error(f'Failed to create md5sums for files in {pid_staging}')
        raise


    logger.info(f'Transferring {pid_staging}/*.tar to nextcloud')
    command = f"scp -r {pid_staging}/*.tar {Config.NEXTCLOUD_HOST}:{Config.NEXTCLOUD_DATA_ROOT}/{Config.NEXTCLOUD_RAW_DIR}/{upload_id}"
    if not runSystemCommand(command, logger, shell=True):
        logger.error(f'Failed to upload {pid_staging}/*.tar to nextcloud')
        raise

    if mode == 'fastq':
        logger.info(f'Transferring {pid_staging}/*.csv to nextcloud')
        command = f"scp -r {pid_staging}/*.csv {Config.NEXTCLOUD_HOST}:{Config.NEXTCLOUD_DATA_ROOT}/{Config.NEXTCLOUD_RAW_DIR}/{upload_id}"
        if not runSystemCommand(command, logger, shell=True):
            logger.error(f'Failed to upload {pid_staging}/*.csv to nextcloud')
            raise

    logger.info(f'Transferring {pid_staging}/*.txt to nextcloud')
    command = f"scp -r {pid_staging}/*.txt {Config.NEXTCLOUD_HOST}:{Config.NEXTCLOUD_DATA_ROOT}/{Config.NEXTCLOUD_RAW_DIR}/{upload_id} "
    if not runSystemCommand(command, logger, shell=True):
        logger.error(f'Failed to upload {pid_staging}/*.txt to nextcloud')
        raise


    transfer_done.touch()

    command = f"scp -r {transfer_done} {Config.NEXTCLOUD_HOST}:{Config.NEXTCLOUD_DATA_ROOT}/{Config.NEXTCLOUD_RAW_DIR}/"
    logger.info(f'Transferring {transfer_done} to nextcloud')
    if not runSystemCommand(command, logger):
        logger.error(f'Failed to upload {transfer_done} to nextcloud')
        raise
    else:
        logger.info(f'Cleaning up {pid_staging}')
        for file in pid_staging.iterdir():
            file.unlink()

    return True

def manageRuns(lims, skip_demux_check):
    machine_aliases = Config.MACHINE_ALIASES
    if Config.DEVMODE: machine_aliases = ['novaseqx_01'] #only used for dev runs

    for machine in machine_aliases:

        machine_dir= Path(f"{Config.CONV_MAIN_DIR}/{machine}")
        md_path = Path(machine_dir)
        for run_dir in md_path.glob("*"):

            if run_dir.name.count('_') != 3 or not run_dir.is_dir(): continue #Not a valid run directory

            #Important Files
            sample_sheet = Path(f'{run_dir}/SampleSheet.csv')
            rta_complete = Path(f'{run_dir}/RTAComplete.txt')
            running_file = Path(f'{run_dir}/.mgr_running')
            failed_file = Path(f'{run_dir}/.mgr_failed')
            done_file = Path(f'{run_dir}/.mgr_done')
            status_file = Path(f'{run_dir}/status.json')
            dx_transfer_done = Path(f'{run_dir}/TransferDone.txt') #Only appears in WGS / WES runs
            log_file = Path(f'{run_dir}/mgr.log')
            # error_file = Path(f'{run_dir}/Conversion/Logs/mgr.err')
            if rta_complete.is_file() and not (running_file.is_file() or failed_file.is_file() or done_file.is_file()): #Run is done and not being processed/has failed/is done
                #Lock directory
                running_file.touch()

                run_info = xml.dom.minidom.parse(f'{run_dir}/RunInfo.xml')
                first_tile = run_info.getElementsByTagName('Tile')[0].firstChild.nodeValue
                first_tile = first_tile.split("_")[-1]

                default_lanes = int(run_info.getElementsByTagName('FlowcellLayout')[0].getAttribute('LaneCount'))
                default_lanes = set(range(1, default_lanes+1))

# <FlowcellLayout LaneCount="1" SurfaceCount="2" SwathCount="6" TileCount="11">
                #Logging set up
                logger = logging.getLogger('Run_Manager')
                logger.setLevel(logging.DEBUG)

                #Set up file log handler
                fh = logging.FileHandler( log_file )
                fh.setLevel(logging.INFO)

                #Set up console log handler
                ch = logging.StreamHandler()
                ch.setLevel(logging.DEBUG)

                #Create and add formatter
                formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
                ch.setFormatter(formatter)
                fh.setFormatter(formatter)

                #Add handlers to log
                logger.addHandler(fh)
                logger.addHandler(ch)

                status = {
                    'run' : {
                        'Stats' : False,
                        'Archive' : False,
                    },
                    'projects' : {
                        # 'Demultiplexing' : False,
                        # 'Transfer-nc' : False,
                        # 'Transfer-hpc' : False,
                    },
                }

                #Check/Set run processing status
                if status_file.is_file():
                    logger.info('Found existing status file')
                    with open(status_file, 'r') as s:
                        status_text = s.read()
                        status = json.loads( status_text )
                        logger.info(f'Current status : {status_text}')

                # updateStatus(status_file, status, 'Demux-check', False) #Make sure status file exists

                flowcell = run_dir.name.split("_")[-1]

                logger.info('Retrieving container name from RunParameters.xml')
                run_parameters_old = Path(f'{run_dir}/runParameters.xml')
                run_parameters = Path(f'{run_dir}/RunParameters.xml')
                if run_parameters_old.is_file():
                    run_parameters_old.rename(run_parameters)

                run_parameters = xml.dom.minidom.parse( run_parameters.open() )
                lims_container_name = None
                if run_parameters.getElementsByTagName('ReagentKitSerial'):  # NextSeq
                    lims_container_name = run_parameters.getElementsByTagName('ReagentKitSerial')[0].firstChild.nodeValue
                elif run_parameters.getElementsByTagName('LibraryTubeSerialBarcode'):  # NovaSeq
                    lims_container_name = run_parameters.getElementsByTagName('LibraryTubeSerialBarcode')[0].firstChild.nodeValue
                elif run_parameters.getElementsByTagName('SerialNumber'):  # iSeq
                    lims_container_name = run_parameters.getElementsByTagName('SerialNumber')[1].firstChild.nodeValue

                logger.info(f'Container name : {lims_container_name}')

                if not sample_sheet.is_file():
                    logger.info('No SampleSheet.csv found')
                    if [x for x in run_dir.glob("*csv")]: #Check if samplesheet with different name exists
                        s = [x for x in run_dir.glob("*csv")][0]
                        logger.info(f'Found {s} , renaming to SampleSheet.csv')
                        s.rename(f'{run_dir}/SampleSheet.csv')
                    else: #No samplesheet found, try to find it in LIMS
                        logger.info(f'Tring to find SampleSheet.csv in LIMS')
                        for reagent_kit_artifact in lims.get_artifacts(containername=lims_container_name):
                            process = reagent_kit_artifact.parent_process

                            for artifact in process.result_files():

                                if 'SampleSheet' in artifact.name and artifact.files:
                                    file_id = artifact.files[0].id
                                    sample_sheet_content = lims.get_file_contents(id=file_id)

                                    with open(sample_sheet, 'w') as sample_sheet_file:
                                        sample_sheet_file.write(sample_sheet_content)

                #Check if samplesheet is now present, if not give error
                if not sample_sheet.is_file():
                    logger.error(f'Failed to find SampleSheet.csv')
                    running_file.unlink()
                    failed_file.touch()
                    statusMail(lims,'Failed (see logs)', run_dir, {}, {})



                #Gather sample/project & lane info from samplesheet & runinfo
                logger.info('Extracting sample / project information from samplesheet')

                run_data = {
                    'lanes' : {}
                }
                project_data = {
                }
                for lane in default_lanes:
                    run_data['lanes'][lane] = {
                        'projects' : set(),
                        'samples' : [],
                    }
                master_sheet = parseSampleSheet(sample_sheet)
                for sample in master_sheet['samples']:
                    pid = sample[ master_sheet['header'].index('Sample_Project') ]
                    if pid not in project_data:
                        project_data[pid] = {
                            'samples' : [],
                            'on_lanes' : set()
                        }
                    project_data[pid]['samples'].append(sample)

                    if 'Lane' in master_sheet['header']:
                        lane = int(sample[ master_sheet['header'].index('Lane') ])
                        project_data[pid]['on_lanes'].add(lane)

                        if lane not in run_data['lanes']:
                            run_data['lanes'][lane] = {
                                'projects' : set(),
                                'samples' : []
                            }
                        run_data['lanes'][lane]['projects'].add(pid)
                        run_data['lanes'][lane]['samples'].append(pid)

                for lane in run_data['lanes']: #Check if each lane has at least 1 projectID, if not add all
                    if not run_data['lanes'][lane]['projects'] and not 'Lane' in master_sheet['header']:
                #         run_data['lanes'][lane]['projects']
                        for pid in project_data:
                            run_data['lanes'][lane]['projects'].add(pid)
                            run_data['lanes'][lane]['samples'].extend( project_data[pid]['samples'] )
                            project_data[pid]['on_lanes'].add(lane)


                #Start processing per projectID unless Demultiplexing for this projectID is already finished
                for pid in project_data:
                    transfer_mode = None
                    if pid not in status['projects']:
                        status['projects'][pid] = {
                            'BCL-Only' : False,
                            'Demultiplexing' : False,
                            'Transfer-nc' : False,
                            'Transfer-hpc' : False,
                        }
                        updateStatus(status_file, status)
                    if not status['projects'][pid]['Demultiplexing'] and not status['projects'][pid]['BCL-Only']:
                        logger.info(f'Starting demultiplexing attempt for projectID {pid}')

                        try:
                            status['projects'][pid]['Demultiplexing'] = demultiplexPID(run_dir, pid, project_data[pid],run_data ,master_sheet,first_tile,logger)
                            if not status['projects'][pid]['Demultiplexing'] : status['projects'][pid]['BCL-Only'] = True
                            updateStatus(status_file, status)
                        except:
                            running_file.unlink()
                            failed_file.touch()
                            statusMail(lims,'Failed (see logs)', run_dir, project_data, run_data)
                            continue

                    if status['projects'][pid]['Demultiplexing']:
                        transfer_mode = 'fastq'
                    elif status['projects'][pid]['BCL-Only']:
                        transfer_mode = 'bcl'

                    if not status['projects'][pid]['Transfer-nc']:
                        try:
                            status['projects'][pid]['Transfer-nc'] = uploadToNextcloud(lims,run_dir,pid, logger, mode=transfer_mode,skip_undetermined=False,lanes=project_data[pid]['on_lanes'])
                            updateStatus(status_file, status)
                        except Exception as e:
                            logger.error(f'Failed to run transfer to Nextcloud\n{traceback.format_exc() }')
                            running_file.unlink()
                            failed_file.touch()
                            statusMail(lims,'Failed (see logs)', run_dir, project_data, run_data)
                            continue

                    if not status['projects'][pid]['Transfer-hpc']:
                        try:
                            status['projects'][pid]['Transfer-hpc'] = uploadToHPC(lims, run_dir, pid,logger)
                            updateStatus(status_file, status)
                        except Exception as e:
                            logger.error(f'Failed to run transfer to HPC\n{traceback.format_exc() }')
                            running_file.unlink()
                            failed_file.touch()
                            statusMail(lims,'Failed (see logs)', run_dir, project_data, run_data)
                            continue


                if not status['run']['Stats']:
                    try:
                        status['run']['Stats'] = generateRunStats(run_dir, logger)

                        updateStatus(status_file, status)
                    except Exception as e:
                        logger.error(f'Failed to create conversion statistics. If this is ok please set ["run"]["Stats"] to True in {status_file} and remove {failed_file}\n{traceback.format_exc() }')
                        running_file.unlink()
                        failed_file.touch()
                        statusMail(lims,'Failed (see logs)', run_dir, project_data, run_data)
                        continue
                if not status['run']['Archive']:
                    try:
                        status['run']['Archive'] = uploadToArchive(run_dir, logger)
                        updateStatus(status_file, status)
                    except Exception as e:
                        logger.error(f'Failed to run transfer to archive storage\n{traceback.format_exc() }')
                        running_file.unlink()
                        failed_file.touch()
                        statusMail(lims,'Failed (see logs)', run_dir, project_data, run_data)
                        continue

                if status['run']['Stats'] and status['run']['Archive']:
                    project_transfers_succesful = True
                    for pid in project_data:
                        if not status['projects'][pid]['Transfer-nc']:
                            project_transfers_succesful = False
                            break
                        if not status['projects'][pid]['Transfer-hpc']:
                            project_transfers_succesful = False
                            break

                    if project_transfers_succesful:
                        # cleanup(run_dir, logger)
                        statusMail(lims,'Finished', run_dir,project_data, run_data)
                        running_file.unlink()
                        done_file.touch()

def statusMail(lims, message, run_dir, project_data, run_data):
    status_file = Path(f'{run_dir}/status.json')
    log_file = Path(f'{run_dir}/mgr.log')
    summary_stats_file = Path(f'{run_dir}/Conversion/Reports/{run_dir.name}_summary.csv')
    demux_stats = Path(f'{run_dir}/Conversion/Reports/Demultiplex_Stats.csv')
    basepercent_by_cycle_plot = Path(f'{run_dir}/Conversion/Reports/{run_dir.name}_BasePercent-by-cycle_BasePercent.png')
    intensity_by_cycle_plot = Path(f'{run_dir}/Conversion/Reports/{run_dir.name}_Intensity-by-cycle_Intensity.png')
    clusterdensity_by_lane_plot = Path(f'{run_dir}/Conversion/Reports/{run_dir.name}_Clusters-by-lane.png')
    flowcell_intensity_plot = Path(f'{run_dir}/Conversion/Reports/{run_dir.name}_flowcell-Intensity.png')
    q_heatmap_plot = Path(f'{run_dir}/Conversion/Reports/{run_dir.name}_q-heat-map.png')
    q_histogram_plot = Path(f'{run_dir}/Conversion/Reports/{run_dir.name}_q-histogram.png')
    multiqc_file = Path(f'{run_dir}/Conversion/Reports/multiqc/{run_dir.name}_multiqc_report.html')


    status = None
    log = None


    with open(status_file, 'r') as s:
        status = json.loads(s.read() )
    with open(log_file, 'r') as l:
        log = l.read()

    expected_reads = getExpectedReads(f'{run_dir}/RunParameters.xml')
    expected_yields = getExpectedYield(f'{run_dir}/RunInfo.xml', expected_reads)
    # print(projectIDs)
    conversion_stats = {}
    summary_stats = {}
    if demux_stats.is_file():
        conversion_stats = parseConversionStats(f'{run_dir}/Conversion/Reports')
    if summary_stats_file.is_file():
        summary_stats = parseSummaryStats(summary_stats_file)


    attachments = {
        #'multiqc_file': str(multiqc_file) if multiqc_file.is_file else None,
        'basepercent_by_cycle_plot': str(basepercent_by_cycle_plot) if basepercent_by_cycle_plot.is_file else None,
        'intensity_by_cycle_plot': str(intensity_by_cycle_plot) if intensity_by_cycle_plot.is_file else None,
        'clusterdensity_by_lane_plot': str(clusterdensity_by_lane_plot) if clusterdensity_by_lane_plot.is_file else None,
        'flowcell_intensity_plot': str(flowcell_intensity_plot) if flowcell_intensity_plot.is_file else None,
        'q_heatmap_plot': str(q_heatmap_plot) if q_heatmap_plot.is_file else None,
        'q_histogram_plot': str(q_histogram_plot) if q_histogram_plot.is_file else None,
    }

    project_names = {}
    for pid in project_data:
        project = Project(lims, id=pid)
        project_name = project.name
        project_names[pid] = project_name

    status_txt = [f'{x}:{project_names[x]}' for x in project_names]
    # print(conversion_stats)
    template_data = {
        'status' : status,
        'log' : log,
        'projects': ",".join(status_txt),
        'run_dir': run_dir.name,
        'nr_reads' : f'{conversion_stats["total_reads"]:,.0f} / {expected_reads:,}' if 'total_reads' in conversion_stats else 0,
        'expected_yields' : expected_yields,
        'conversion_stats': conversion_stats,
        'summary_stats' : summary_stats,
        'run_data' : run_data,
    }

        # 'project_data' : project_data,
        #


    mail_content = renderTemplate('conversion_status_mail.html', template_data)
    if status_txt:
        mail_subject = f'[USEQ] Status ({", ".join(status_txt)}): {message}'
    else:
        mail_subject = f'[USEQ] Status ({run_dir}): {message}'
    sendMail(mail_subject,mail_content, Config.MAIL_SENDER ,Config.MAIL_ADMINS, attachments=attachments)


def zipConversionReport(run_dir,logger):
    """Zip conversion reports."""
    logger.info('Zipping conversion report')
    zip_file = f'{run_dir}/{run_dir.name}_Reports.zip'
    os.chdir(f'{run_dir}/Conversion/')

    command = f'zip -FSr {zip_file} Reports/ '
    if not runSystemCommand(command, logger):
        logger.error('Failed to zip conversion report')
        raise

    return zip_file

def run(lims, skip_demux_check=False):
    """Runs the manageRuns function"""

    #Set up nextcloud
    global nextcloud_util
    nextcloud_util = NextcloudUtil()
    nextcloud_util.setHostname( Config.NEXTCLOUD_HOST )
    nextcloud_util.setup( Config.NEXTCLOUD_USER, Config.NEXTCLOUD_PW, Config.NEXTCLOUD_WEBDAV_ROOT,Config.NEXTCLOUD_RAW_DIR,Config.MAIL_SENDER )


    manageRuns(lims, skip_demux_check )
