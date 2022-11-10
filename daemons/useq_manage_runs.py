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

from genologics.entities import Project
from itertools import islice
from pathlib import Path
from modules.useq_illumina_parsers import getExpectedReads,parseSampleSheet
from modules.useq_nextcloud import NextcloudUtil
from modules.useq_template import TEMPLATE_PATH,TEMPLATE_ENVIRONMENT,renderTemplate
from modules.useq_mail import sendMail
from config import Config

def addFlowcellToFastq(run_dir, flowcell_id, logger):
    """Add flowcell id to fastq.gz filename."""
    logger.info('Adding flowcell to fastqs')

    base_calls_dir = Path(f'{run_dir}/Conversion/FastQ')
    for fastq in base_calls_dir.rglob("*.fastq.gz"):
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


def demuxCheck(run_dir,logger):

    skip_demux = False

    sample_sheet = Path(f'{run_dir}/SampleSheet.csv')
    sample_sheet_parsed = parseSampleSheet( sample_sheet )

    sample_sheet_rev =  Path(f'{run_dir}/Conversion/Demux-check/SampleSheet-rev.csv')
    samples = sample_sheet_parsed['samples']

    rev_samples = []
    header = sample_sheet_parsed['header']

    run_info = xml.dom.minidom.parse(f'{run_dir}/RunInfo.xml')
    first_tile = run_info.getElementsByTagName('Tile')[0].firstChild.nodeValue
    first_tile = first_tile.split("_")[-1]


    if len(samples) > 1:
        # With 1 sample customers probably want the BCL files, demux will clear this up. For more samples cleanup samplesheet before demux
        logger.info('Found more than 1 sample in samplesheet, attempting cleanup')
        for sample in samples:
            dual_index = False
            if 'N' in sample[header.index('index')] and re.search("[ACGT]", sample[header.index('index')] ):#Cleanup UMI NN's from first index
                sample[header.index('index')] = sample[header.index('index')].replace("N","")
            if 'index2' in header: dual_index = True

            sample_rev = sample.copy()
            if dual_index:
                revseq = revcomp(sample_rev[header.index('index2')])
                sample_rev[header.index('index2')] = revseq
            else:
                revseq = revcomp(sample_rev[header.index('index')])
                sample_rev[header.index('index')] = revseq
            rev_samples.append(sample_rev)
        logger.info('Creating cleaned up samplesheet')
        writeSampleSheet(sample_sheet, header, samples, sample_sheet_parsed['top'])
        logger.info('Creating reverse complemented samplesheet')
        writeSampleSheet(sample_sheet_rev, header, rev_samples, sample_sheet_parsed['top'])
    elif len(samples) == 1:
        logger.info('Found only 1 sample in samplesheet, attempting cleanup')
        sample = samples[0]
        if 'N' in sample[header.index('index')] and re.search("[ACGT]", sample[header.index('index')] ):#Cleanup UMI NN's from first index
            sample[header.index('index')] = sample[header.index('index')].replace("N","")
        sample[header.index('index')] = sample[header.index('index')].replace("N","A")
        if 'index2' in header:
            sample[header.index('index2')] = sample[header.index('index2')].replace("N","A")
        logger.info('Creating cleaned up samplesheet')
        writeSampleSheet(sample_sheet, header, samples, sample_sheet_parsed['top'])
        logger.info('Creating reverse complemented samplesheet')
        writeSampleSheet(sample_sheet_rev, header, samples, sample_sheet_parsed['top'])

    #Samplesheet is OK first try
    logger.info('Running demultiplexing check on original samplesheet')

    command = f'{Config.CONV_BCLCONVERT}/bcl-convert --bcl-input-directory {run_dir} --output-directory {run_dir}/Conversion/Demux-check/1 --sample-sheet {sample_sheet} --bcl-sampleproject-subdirectories true --force --tiles {first_tile}'
    if not runSystemCommand(command, logger):
        logger.error('Failed to run 1st demultiplexing check')
        raise

    logger.info('Checking demultiplexing stats for original samplesheet')
    stats = parseConversionStats(f'{run_dir}/Conversion/Demux-check/1/Reports')
    if stats['undetermined_reads'] / stats['total_reads'] < 0.40:
        logger.info('Demultiplexing stats for original samplesheet passed')
        return True

    #Try revcomp samplesheet
    logger.info('Running demultiplexing check on reverse complemented samplesheet')
    command = f'{Config.CONV_BCLCONVERT}/bcl-convert --bcl-input-directory {run_dir} --output-directory {run_dir}/Conversion/Demux-check/2 --sample-sheet {sample_sheet_rev} --bcl-sampleproject-subdirectories true --force --tiles {first_tile}'
    if not runSystemCommand(command, logger):
        logger.error('Failed to run 2nd demultiplexing check')
        raise

    logger.info('Checking demultiplexing stats for reverse complemented samplesheet')
    stats = parseConversionStats(f'{run_dir}/Conversion/Demux-check/2/Reports')

    if stats['undetermined_reads'] / stats['total_reads'] < 0.40:
        logger.info('Demultiplexing stats for reverse complemented samplesheet passed')
        logger.info('Replacing original samplesheet with reverse complemented version')

        sample_sheet_rev.rename(sample_sheet)

        return True

    return False

def filterStats(lims, pid, pid_staging, report_dir):
    samples = lims.get_samples(projectlimsid=pid)
    sample_names = [x.name for x in samples]
    adapter_metrics = Path(f'{report_dir}/Adapter_Metrics.csv')
    adapter_metrics_filtered = Path(f'{pid_staging}/Adapter_Metrics.csv')
    demultiplex_stats = Path(f'{report_dir}/Demultiplex_Stats.csv')
    demultiplex_stats_filtered = Path(f'{pid_staging}/Demultiplex_Stats.csv')

    with open(adapter_metrics, 'r') as original, open(adapter_metrics_filtered, 'w') as filtered:
        for line in original.readlines():
            parts = line.split(',')
            if line.startswith('Lane') or parts[1] in sample_names:
                filtered.write(line)

    with open(demultiplex_stats, 'r') as original, open(demultiplex_stats_filtered, 'w') as filtered:
        for line in original.readlines():
            parts = line.split(',')
            if line.startswith('Lane') or parts[1] in sample_names:
                filtered.write(line)


def generateRunStats(run_dir, logger):
    """Create run stats files using interop tool."""
    stats_dir = Path(f'{run_dir}/Conversion/Reports')

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

    except subprocess.CalledProcessError as e:
        logger.error(
            f'Failed to generate run plots'
            f'Returned {e.returncode}\n{e}\n'
            f'{e.stderr}\n'
        )

def getExpectedYield(run_info_xml, expected_reads):
    run_info = xml.dom.minidom.parse(run_info_xml)

    yields = { 'r1':0,'r2':0 }

    for read in run_info.getElementsByTagName('Read'):
        if read.getAttribute('IsIndexedRead') == 'N':
            if int(read.getAttribute('Number')) == 1:
                yields['r1'] = int( read.getAttribute('NumCycles')) * expected_reads
            else:
                yields['r2'] = int( read.getAttribute('NumCycles')) * expected_reads
    return yields

# first_tile = run_info.getElementsByTagName('Tile')[0].firstChild.nodeValue
    # <Reads>
    # 	<Read Number="1" NumCycles="51" IsIndexedRead="N" IsReverseComplement="N"/>
    # 	<Read Number="2" NumCycles="8" IsIndexedRead="Y" IsReverseComplement="N"/>
    # 	<Read Number="3" NumCycles="51" IsIndexedRead="N" IsReverseComplement="N"/>
    # </Reads>


def parseConversionStats(dir):
    demux_stats = f'{dir}/Demultiplex_Stats.csv'
    qual_metrics = Path(f'{dir}/Quality_Metrics.csv')
    top_unknown = f'{dir}/Top_Unknown_Barcodes.csv'
    stats = {
        'total_reads' : 0,
        'undetermined_reads' : 0,
        'samples' : [],
        'top_unknown' : []
    }
    samples_tmp = {}
    with open(demux_stats, 'r') as d:
        csv_reader = csv.DictReader(d)
        for row in csv_reader:
            if row['SampleID'] == 'Undetermined':
                stats['undetermined_reads'] += float(row['# Reads'])
                stats['total_reads'] += float(row['# Reads'])
            else:
                stats['total_reads'] += float(row['# Reads'])

            if row['SampleID'] not in samples_tmp:
                samples_tmp[ row['SampleID'] ] = {
                    'Index' : None,
                    '# Reads' : 0,
                    '# Perfect Index Reads' : 0,
                    '# One Mismatch Index Reads' : 0,
                }
            samples_tmp[ row['SampleID'] ]['Index'] = row['Index']
            samples_tmp[ row['SampleID'] ]['Lane'] = int(row['Lane'])
            samples_tmp[ row['SampleID'] ]['# Reads'] += int(row['# Reads'])
            samples_tmp[ row['SampleID'] ]['# Perfect Index Reads']  += int(row['# Perfect Index Reads'])
            samples_tmp[ row['SampleID'] ]['# One Mismatch Index Reads']  += int(row['# One Mismatch Index Reads'])
    if qual_metrics.is_file():
        with open(qual_metrics,'r') as q:
            csv_reader = csv.DictReader(q)
            for row in csv_reader:

                mqs = f'Read {row["ReadNumber"]} Mean Quality Score (PF)'
                q30 = f'Read {row["ReadNumber"]} % Q30'
                if mqs not in samples_tmp[ row['SampleID'] ]:
                    samples_tmp[ row['SampleID'] ][mqs] = 0
                if q30 not in samples_tmp[ row['SampleID'] ]:
                    samples_tmp[ row['SampleID'] ][q30] = 0

                samples_tmp[ row['SampleID'] ][mqs] += float(row['Mean Quality Score (PF)'])
                samples_tmp[ row['SampleID'] ][q30] += float(row['% Q30'])


    for sampleID in samples_tmp:

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

    with open(top_unknown, 'r') as t:
        csv_reader = csv.DictReader(t)
        for row in islice(csv_reader,0,20):
            stats['top_unknown'].append(row)
    return stats

def parseSummaryStats( summary ):
    stats = []
    with open(summary, 'r') as sumcsv:
        lines = sumcsv.readlines()
        line_nr = 0
        while line_nr < len(lines):
            line = lines[line_nr].rstrip()
            if not line: line_nr+=1;continue
            if line.startswith('Level'):
                header = [x.rstrip() for x in line.split(",")]
                for sub_line in lines[line_nr+1:]:
                    # print (line)
                    cols = [x.rstrip() for x in sub_line.split(",")]
                    stats.append(dict(zip(header,cols)))
                    if sub_line.startswith('Total'): break
                break
            else:
                line_nr += 1

    return stats


def revcomp(seq):
    revcompl = lambda x: ''.join([{'A':'T','C':'G','G':'C','T':'A'}[B] for B in x][::-1])
    return revcompl(seq)

def runSystemCommand(command, logger, shell=False):
    #split command into pieces for subprocess.run
    command_pieces = shlex.split(command)
    print(command_pieces)
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



def updateStatus(file, status, step, bool):

    status[step] = bool
    with open(file, 'w') as f:
        f.write(json.dumps(status))

def uploadToArchive(run_dir, logger):

    machine = run_dir.parents[0].name

    logger.info('Uploading run folder to archive storage')
    command = f"rsync -rahm --exclude '*jpg' --exclude '*fastq.gz' --exclude '*fq.gz' {run_dir} {Config.USEQ_USER}@{Config.HPC_TRANSFER_SERVER}:{Config.HPC_ARCHIVE_DIR}/{machine}"

    if not runSystemCommand(command, logger):
        logger.error(f'Failed upload run folder to archive storage')
        raise

    return True

def uploadToHPC(lims, run_dir, projectIDs, logger):
    machine = run_dir.parents[0].name
    to_sync = ''
    command = '/usr/bin/rsync -rah --update --stats --verbose --prune-empty-dirs '
    for pid in projectIDs:
        project = Project(lims, id=pid)
        project_name = project.name

        samples = lims.get_samples(projectlimsid=project.id)
        analysis_steps = samples[0].udf['Analysis'].split(',')
        if len(analysis_steps) > 1:
            command += f'--include "Conversion/FastQ/{pid}/*.fastq.gz" '
        else:
            command += f'--exclude "Conversion/FastQ/{pid}/*.fastq.gz" '

    command += " --include '*/' --include 'md5sum.txt' --include 'SampleSheet.csv' --include 'RunInfo.xml' --include '*unParameters.xml' --include 'InterOp/**' --include '*/Conversion/Reports/**' --include '*/FastQ/Reports/**' --include 'Data/Intensities/BaseCalls/Stats/**' --include '*.[pP][eE][dD]'"
    command += " --exclude '*'"
    command += f" {run_dir}"
    command += f" {Config.USEQ_USER}@{Config.HPC_TRANSFER_SERVER}:/{Config.HPC_RAW_ROOT}/{machine}"

    logger.info('Uploading run folder to HPC')

    if not runSystemCommand(command, logger):
        logger.error(f'Failed upload run folder to HPC')
        raise

    return True

def uploadToNextcloud(lims, run_dir, mode,projectIDs,logger):
    machine = run_dir.parents[0].name

    #Create .tar files for upload to nextcloud
    if mode == 'fastq':
        for pid in projectIDs:
            pid_staging = Path(f'{Config.CONV_STAGING_DIR}/{pid}')
            pid_staging.mkdir(parents=True, exist_ok=True)

            tmp_dir = Path(f'{run_dir}/{pid}')
            tmp_dir.mkdir(parents=True, exist_ok=True)

            logger.info(f'Creating nextcloud dir for {pid}')
            command = f"scp -r {tmp_dir} {Config.NEXTCLOUD_HOST}:{Config.NEXTCLOUD_DATA_ROOT}/{Config.NEXTCLOUD_RAW_DIR}/"
            if not runSystemCommand(command, logger):
                logger.error(f'Failed to create nextcloud dir for {pid}')
                tmp_dir.rmdir()
                raise

            tmp_dir.rmdir()

            pid_samples  = set()
            pid_dir = Path(f'{run_dir}/Conversion/FastQ/{pid}')
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

            if len(projectIDs) == 1:
                logging.info(f'Zipping undetermined reads')
                und_zip = Path(f'{pid_staging}/undetermined.tar')
                und_zip_done = Path(f'{pid_staging}/undetermined.tar.done')
                if not und_zip_done.is_file():
                    os.chdir(f'{run_dir}/Conversion/FastQ/')
                    command = f'tar -cvf {und_zip} undetermined_*fastq.gz'
                    if not runSystemCommand(command, logger, shell=True):
                        logger.error(f'Failed to create {und_zip}')
                        raise
                    else:
                        und_zip_done.touch()

            logger.info(f'Filtering stats for {pid}')
            report_dir = Path(f'{run_dir}/Conversion/Reports')
            filterStats(lims, pid, pid_staging, report_dir)

    else:
        pid = list(projectIDs)[0]
        pid_staging = Path(f'{Config.CONV_STAGING_DIR}/{pid}')
        pid_staging.mkdir(parents=True, exist_ok=True)

        tmp_dir = Path(f'{run_dir}/{pid}')
        tmp_dir.mkdir(parents=True, exist_ok=True)


        logger.info(f'Creating nextcloud dir for {pid}')
        command = f"scp -r {tmp_dir} {Config.NEXTCLOUD_HOST}:{Config.NEXTCLOUD_DATA_ROOT}/{Config.NEXTCLOUD_RAW_DIR}/"
        if not runSystemCommand(command, logger):
            logger.error(f'Failed to create nextcloud dir for {pid}')
            tmp_dir.rmdir()
            raise

        tmp_dir.rmdir()

        zipped_run = Path(f'{pid_staging}/{pid}.tar')
        zip_done = Path(f'{pid_staging}/{pid}.tar.done')



        if not zip_done.is_file():
            logger.info(f'Zipping {run_dir.name} to {zipped_run}')
            os.chdir(run_dir.parents[0])
            command = f'tar -cvf {zipped_run} --exclude "Conversion/" --exclude "*fastq.gz*" --exclude "*run_zip.*" {run_dir.name}'
            if not runSystemCommand(command, logger, shell=True):
                logger.error(f'Failed to zip {run_dir.name} to {zipped_run}')
                raise
            else:
                zip_done.touch()


    #Upload .tar/stats & md5sums to nextcloud
    for pid in projectIDs:
        pid_staging = Path(f'{Config.CONV_STAGING_DIR}/{pid}')
        transfer_done = Path(f'{Config.CONV_STAGING_DIR}/{pid}.done')

        logger.info(f'Creating md5sums for {pid}')
        os.chdir(pid_staging)
        command = f'md5sum *.tar > {pid_staging}/md5sums.txt'
        if not runSystemCommand(command, logger, shell=True):
            logger.error(f'Failed to create md5sums for files in {pid_staging}')
            raise


        logger.info(f'Transferring {pid_staging}/*.tar to nextcloud')
        command = f"scp -r {pid_staging}/*.tar {Config.NEXTCLOUD_HOST}:{Config.NEXTCLOUD_DATA_ROOT}/{Config.NEXTCLOUD_RAW_DIR}/{pid}"
        if not runSystemCommand(command, logger, shell=True):
            logger.error(f'Failed to upload {pid_staging}/*.tar to nextcloud')
            raise

        if mode == 'fastq':
            logger.info(f'Transferring {pid_staging}/*.csv to nextcloud')
            command = f"scp -r {pid_staging}/*.csv {Config.NEXTCLOUD_HOST}:{Config.NEXTCLOUD_DATA_ROOT}/{Config.NEXTCLOUD_RAW_DIR}/{pid}"
            if not runSystemCommand(command, logger, shell=True):
                logger.error(f'Failed to upload {pid_staging}/*.csv to nextcloud')
                raise

        logger.info(f'Transferring {pid_staging}/*.txt to nextcloud')
        command = f"scp -r {pid_staging}/*.txt {Config.NEXTCLOUD_HOST}:{Config.NEXTCLOUD_DATA_ROOT}/{Config.NEXTCLOUD_RAW_DIR}/{pid} "
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

def manageRuns(lims):
    for machine in Config.MACHINE_ALIASES:
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
            log_file = Path(f'{run_dir}/mgr.log')
            # error_file = Path(f'{run_dir}/Conversion/Logs/mgr.err')
            if rta_complete.is_file() and not (running_file.is_file() or failed_file.is_file() or done_file.is_file()): #Run is done and not being processed/has failed/is done
                #Lock directory
                running_file.touch()

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
                    'Demux-check' : False,
                    'Conversion' : False,
                    'Transfer-nc' : False,
                    'Transfer-hpc' : False,
                    'Archive' : False,
                }
                try:
                    #Set up important directories
                    logger.info('Creating conversion directories')
                    Path(f'{run_dir}/Conversion/Reports').mkdir(parents=True, exist_ok=True)
                    Path(f'{run_dir}/Conversion/FastQ').mkdir(parents=True, exist_ok=True)
                    Path(f'{run_dir}/Conversion/Demux-check').mkdir(parents=True, exist_ok=True)
                except :
                    logger.error(f'Failed to create conversion directories:\n{traceback.format_exc() }')
                    running_file.unlink()
                    failed_file.touch()
                    statusMail(lims,'Failed (see logs)', run_dir, [])
                    continue

                #Check/Set run processing status
                if status_file.is_file():
                    logger.info('Found existing status file')
                    with open(status_file, 'r') as s:
                        status_text = s.read()
                        status = json.loads( status_text )
                        logger.info(f'Current status : {status_text}')
                updateStatus(status_file, status, 'Demux-check', False) #Make sure status file exists

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
                    statusMail(lims,'Failed (see logs)', run_dir, [])
                    continue


                #Group the samples per projectID (needed for combined runs)
                logger.info('Extracting sample / project information from samplesheet')
                projectIDs = set()
                sheet = parseSampleSheet(sample_sheet)
                for sample in sheet['samples']:
                    projectIDs.add( sample[ sheet['header'].index('Sample_Project') ] )


                #If demultiplexing check was not succesfull/not done
                if status['Demux-check'] == False:
                    logger.info('Starting demultiplexing check')
                    try:
                        status['Demux-check']  = demuxCheck( run_dir, logger )
                        updateStatus(status_file, status, 'Demux-check', status['Demux-check'])
                    except :
                        logger.error(f'Failed to run demultiplexing check\n{traceback.format_exc() } ')
                        running_file.unlink()
                        failed_file.touch()
                        statusMail(lims,'Failed (see logs)', run_dir, projectIDs)
                        continue


                if status['Demux-check']: #Demultiplexing will probably succeed, so carry on

                    if not status['Conversion']:
                        logger.info('Starting demultiplexing')

                        command = f'{Config.CONV_BCLCONVERT}/bcl-convert --bcl-input-directory {run_dir} --output-directory {run_dir}/Conversion/FastQ --bcl-sampleproject-subdirectories true --force --sample-sheet {sample_sheet} '
                        if not runSystemCommand(command, logger):
                            logger.error(f'Failed to run demultiplexing\n')
                            running_file.unlink()
                            failed_file.touch()
                            statusMail(lims,'Failed (see logs)', run_dir, projectIDs)
                            continue



                        logger.info('Moving Conversion/FastQ/Reports to Conversion/Reports')
                        try:
                            stats_files = os.listdir(f'{run_dir}/Conversion/FastQ/Reports/')
                            for f in stats_files:
                                shutil.move(os.path.join(f'{run_dir}/Conversion/FastQ/Reports/', f), f'{run_dir}/Conversion/Reports')
                        except :
                            logger.error(f'Failed to move Conversion/FastQ/Reports to Conversion/Reports\n{traceback.format_exc() }')
                            running_file.unlink()
                            failed_file.touch()
                            statusMail(lims,'Failed (see logs)', run_dir, projectIDs)
                            continue



                        command = f'rm -r {run_dir}/Conversion/FastQ/Reports'
                        if not runSystemCommand(command, logger):
                            logger.error(f'Failed to remove Conversion/FastQ/Reports \n ')
                            running_file.unlink()
                            failed_file.touch()
                            statusMail(lims,'Failed (see logs)', run_dir, projectIDs)
                            continue

                        try:
                            generateRunStats(run_dir, logger)
                        except Exception as e:
                            logger.error(f'Failed to create conversion statistics. If this is ok please set Conversion to True in {status_file} and remove {failed_file}\n{traceback.format_exc() }')
                            running_file.unlink()
                            failed_file.touch()
                            statusMail(lims,'Failed (see logs)', run_dir, projectIDs)
                            continue
##

                        addFlowcellToFastq(run_dir, flowcell, logger)
                        # try:
                        #     md5sumFastq(run_dir,logger)
                        # except Exception as e:
                        #     logger.error(f'Failed to create FastQ md5sums\n{traceback.format_exc() } ')
                        #     running_file.unlink()
                        #     failed_file.touch()
                        #     statusMail(lims,'Failed (see logs)', run_dir, projectIDs)
                        #     continue


                        try:
                            zip_file = zipConversionReport(run_dir, logger)
                        except Exception as e:
                            logger.error(f'Failed to zip the conversion report\n{traceback.format_exc() } ')
                            running_file.unlink()
                            failed_file.touch()
                            statusMail(lims,'Failed (see logs)', run_dir, projectIDs)
                            continue

                        updateStatus(status_file, status, 'Conversion', True)

                    if status['Conversion']: #Conversion succesful,
                        if not status['Transfer-nc']:
                            try:
                                status['Transfer-nc'] = uploadToNextcloud(lims,run_dir, 'fastq',projectIDs, logger)
                                updateStatus(status_file, status, 'Transfer-nc', status['Transfer-nc'])
                            except Exception as e:
                                logger.error(f'Failed to run transfer to Nextcloud\n{traceback.format_exc() }')
                                running_file.unlink()
                                failed_file.touch()
                                statusMail(lims,'Failed (see logs)', run_dir, projectIDs)
                                continue

                        if not status['Transfer-hpc']:
                            try:
                                status['Transfer-hpc'] = uploadToHPC(lims, run_dir, projectIDs,logger)
                                updateStatus(status_file, status, 'Transfer-hpc', status['Transfer-hpc'])
                            except Exception as e:
                                logger.error(f'Failed to run transfer to HPC\n{traceback.format_exc() }')
                                running_file.unlink()
                                failed_file.touch()
                                statusMail(lims,'Failed (see logs)', run_dir, projectIDs)
                                continue
                else: #Skip straight to transfer
                    logger.info(f'Demultiplexing check failed, skipping demultiplexing and uploading BCL files to nextcloud instead')
                    try:
                        generateRunStats(run_dir, logger)
                    except Exception as e:
                        logger.error(f'Failed to create run statistics. The sequencing run probably failed.\n{traceback.format_exc() }')
                        running_file.unlink()
                        failed_file.touch()
                        statusMail(lims,'Failed (see logs)', run_dir, projectIDs)
                        continue

                    if not status['Transfer-nc']:
                        try:
                            status['Transfer-nc'] = uploadToNextcloud(lims,run_dir, 'bcl',projectIDs,logger)
                            updateStatus(status_file, status, 'Transfer-nc',status['Transfer-nc'])
                        except Exception as e:
                            logger.error(f'Failed to run transfer to Nextcloud\n{traceback.format_exc() }')
                            running_file.unlink()
                            failed_file.touch()
                            statusMail(lims,'Failed (see logs)', run_dir, projectIDs)
                            continue

                    if not status['Transfer-hpc']:
                        try:
                            status['Transfer-hpc'] = uploadToHPC(lims, run_dir, projectIDs,logger)
                            updateStatus(status_file, status, 'Transfer-hpc',status['Transfer-hpc'])
                        except Exception as e:
                            logger.error(f'Failed to run transfer to HPC\n{traceback.format_exc() }')
                            running_file.unlink()
                            failed_file.touch()
                            statusMail(lims,'Failed (see logs)', run_dir, projectIDs)
                            continue

                if not status['Archive']:
                    try:
                        status['Archive'] = uploadToArchive(run_dir, logger)
                        updateStatus(status_file, status, 'Archive',status['Archive'])
                    except Exception as e:
                        logger.error(f'Failed to run transfer to archive storage\n{traceback.format_exc() }')
                        running_file.unlink()
                        failed_file.touch()
                        statusMail(lims,'Failed (see logs)', run_dir, projectIDs)
                        continue

###

                if status['Conversion'] and status['Transfer-nc'] and status['Transfer-hpc'] and status['Archive']:
                    cleanup(run_dir, logger)
                    statusMail(lims,'Finished', run_dir, projectIDs)
                    running_file.unlink()
                    done_file.touch()
                elif status['Transfer-nc'] and status['Transfer-hpc'] and status['Archive']:
                    cleanup(run_dir, logger)
                    statusMail(lims,'Finished', run_dir,projectIDs)
                    running_file.unlink()
                    done_file.touch()

def statusMail(lims, message, run_dir, projectIDs):
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


    status = None
    log = None


    with open(status_file, 'r') as s:
        status = json.loads(s.read() )
    with open(log_file, 'r') as l:
        log = l.read()

    expected_reads = getExpectedReads(f'{run_dir}/RunParameters.xml')
    expected_yields = getExpectedYield(f'{run_dir}/RunInfo.xml', expected_reads)

    conversion_stats = {}
    summary_stats = {}
    if demux_stats.is_file():
        conversion_stats = parseConversionStats(f'{run_dir}/Conversion/Reports')
    if summary_stats_file.is_file():
        summary_stats = parseSummaryStats(summary_stats_file)


    attachments = {
        # 'zip_file': f'{run_dir}/{run_dir.name}_Reports.zip',
        'basepercent_by_cycle_plot': str(basepercent_by_cycle_plot) if basepercent_by_cycle_plot.is_file else None,
        'intensity_by_cycle_plot': str(intensity_by_cycle_plot) if intensity_by_cycle_plot.is_file else None,
        'clusterdensity_by_lane_plot': str(clusterdensity_by_lane_plot) if clusterdensity_by_lane_plot.is_file else None,
        'flowcell_intensity_plot': str(flowcell_intensity_plot) if flowcell_intensity_plot.is_file else None,
        'q_heatmap_plot': str(q_heatmap_plot) if q_heatmap_plot.is_file else None,
        'q_histogram_plot': str(q_histogram_plot) if q_histogram_plot.is_file else None,
    }

    project_names = {}
    for pid in projectIDs:
        project = Project(lims, id=pid)
        project_name = project.name
        project_names[pid] = project_name

    status_txt = [f'{x}:{project_names[x]}' for x in project_names]

    template_data = {
        'status' : status,
        'log' : log,
        'projects': ",".join(status_txt),
        'run_dir': run_dir.name,
        'nr_reads' : f'{conversion_stats["total_reads"]:,} / {expected_reads:,}' if 'total_reads' in conversion_stats else 0,
        'expected_yields' : expected_yields,
        'conversion_stats': conversion_stats,
        'summary_stats' : summary_stats
    }




    mail_content = renderTemplate('conversion_status_mail.html', template_data)
    if status_txt:
        mail_subject = f'[USEQ] Status ({", ".join(status_txt)}): {message}'
    else:
        mail_subject = f'[USEQ] Status ({run_dir}): {message}'
    sendMail(mail_subject,mail_content, Config.MAIL_SENDER ,Config.MAIL_ADMINS, attachments=attachments)

def writeSampleSheet(samplesheet, header, samples, top):

    with open(samplesheet, 'w') as new_sheet:
        new_sheet.write(top)
        new_sheet.write(f'{",".join(header)}\n')
        for sample in samples:
            new_sheet.write(f'{",".join(sample)}\n')

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

def run(lims):
    """Runs the manageRuns function"""

    #Set up nextcloud
    global nextcloud_util
    nextcloud_util = NextcloudUtil()
    nextcloud_util.setHostname( Config.NEXTCLOUD_HOST )
    nextcloud_util.setup( Config.NEXTCLOUD_USER, Config.NEXTCLOUD_PW, Config.NEXTCLOUD_WEBDAV_ROOT,Config.NEXTCLOUD_RAW_DIR,Config.MAIL_SENDER )


    manageRuns(lims )
