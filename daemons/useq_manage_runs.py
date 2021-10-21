import os
import argparse
import xml.dom.minidom
import re
import json
import csv
from config import DATA_DIRS_RAW,DATA_DIR_HPC,ARCHIVE_DIR,MAIL_SENDER,MAIL_ADMINS,INTEROP_PATH,RUNTYPE_YIELDS,BCLCONVERT_PATH,BCLCONVERT_PROCESSING_THREADS,BCLCONVERT_WRITING_THREADS,STAGING_DIR,NEXTCLOUD_DATA_ROOT,NEXTCLOUD_PW,NEXTCLOUD_USER,NEXTCLOUD_HOST,NEXTCLOUD_RAW_DIR,NEXTCLOUD_WEBDAV_ROOT
from modules.useq_mail import sendMail
from modules.useq_illumina_parsers import getExpectedReads,parseSampleSheet
from modules.useq_nextcloud import NextcloudUtil
from pathlib import Path
from modules.useq_template import TEMPLATE_PATH,TEMPLATE_ENVIRONMENT,renderTemplate
from datetime import datetime
from genologics.entities import Project
from itertools import islice
def revcomp(seq):
    revcompl = lambda x: ''.join([{'A':'T','C':'G','G':'C','T':'A'}[B] for B in x][::-1])
    return revcompl(seq)

def convertBCL(run_dir, sample_sheet, log_file, error_file):
    updateLog(log_file, 'Conversion : Running')
    """Convert bcl files from run to fastq.gz files."""

    # Start conversion
    os.system(f'date >> {log_file}')
    command = f'{BCLCONVERT_PATH}/bcl-convert --bcl-input-directory {run_dir} --output-directory {run_dir}/Conversion/FastQ --bcl-sampleproject-subdirectories true --force --sample-sheet {sample_sheet}'
    command = f'{command} 1>> /dev/null 2>> {error_file}'
    exit_code = os.system(command)
    updateLog(log_file, 'Conversion : Done')
    return exit_code
#
def addFlowcellToFastq(run_dir, flowcell_id,log_file, error_file):
    updateLog(log_file, 'Adding flowcell to fastqs : Running')
    """Add flowcell id to fastq.gz filename."""
    base_calls_dir = Path(f'{run_dir}/Conversion/FastQ')
    for fastq in base_calls_dir.rglob("*.fastq.gz"):
        filename_parts = fastq.name.split('_')
        if filename_parts[1] != flowcell_id:
            filename_parts.insert(1, flowcell_id)
            new_filename = '_'.join(filename_parts)
            fastq.rename(f'{fastq.parent}/{new_filename}')
    updateLog(log_file, 'Adding flowcell to fastqs : Done')

def zipConversionReport(run_dir,log_file, error_file):
    updateLog(log_file, 'Zipping conversion report : Running')
    """Zip conversion reports."""
    zip_file = f'{run_dir}/{run_dir.name}_Reports.zip'
    os.chdir(f'{run_dir}/Conversion/')
    os.system(f'zip -FSr {zip_file} Reports/ 1 > /dev/null ')
    updateLog(log_file, 'Zipping conversion report : Done')
    return zip_file
#
def md5sumFastq(run_dir, log_file, error_file):
    updateLog(log_file, 'Create FastQ md5sums : Running')
    """Generate md5sums for all fastq.gz files from a sequencing run."""
    command = f'(cd {run_dir}/Conversion/FastQ && find . -type f -iname "*.fastq.gz" -exec md5sum {{}} \\; > md5sum.txt)'
    os.system(command)
    updateLog(log_file, 'Create FastQ md5sums : Done')

def generateRunStats(run_dir, log_file, error_file):
    """Create run stats files using interop tool."""
    stats_dir = Path(f'{run_dir}/Conversion/Reports')

    exit_codes = []
    os.chdir(stats_dir)
    updateLog(log_file, 'Generate run stats : Running')
    # Run summary csv
    exit_codes.append(os.system(f'{INTEROP_PATH}/bin/summary {run_dir} > {stats_dir}/{run_dir.name}_summary.csv'))
    # Index summary csv
    exit_codes.append(os.system(f'{INTEROP_PATH}/bin/index-summary {run_dir} --csv= > {stats_dir}/{run_dir.name}_index-summary.csv'))
    # Intensity by cycle plot
    exit_codes.append(os.system(f'{INTEROP_PATH}/bin/plot_by_cycle {run_dir} --metric-name=Intensity | gnuplot'))
    # % Base by cycle plot
    exit_codes.append(os.system(f'{INTEROP_PATH}/bin/plot_by_cycle {run_dir} --metric-name=BasePercent | gnuplot'))
    # Clustercount by lane plot
    exit_codes.append(os.system(f'{INTEROP_PATH}/bin/plot_by_lane {run_dir} --metric-name=Clusters | gnuplot'))
    # Flowcell intensity plot
    exit_codes.append(os.system(f'{INTEROP_PATH}/bin/plot_flowcell {run_dir} | gnuplot'))
    # QScore heatmap plot
    exit_codes.append(os.system(f'{INTEROP_PATH}/bin/plot_qscore_heatmap {run_dir} | gnuplot'))
    # QScore histogram plot
    exit_codes.append(os.system(f'{INTEROP_PATH}/bin/plot_qscore_histogram {run_dir} | gnuplot'))
    #print (exit_codes)
    exit_codes.append(os.system(f'multiqc {run_dir}/Conversion/FastQ {run_dir}/Conversion/Logs {run_dir}/Conversion/Reports -o {stats_dir} -k json --quiet 1>> /dev/null 2>> {error_file}'))
    updateLog(log_file, 'Generate run stats : Done')
    return exit_codes

def writeSampleSheet(samplesheet, header, samples, top):

    with open(samplesheet, 'w') as new_sheet:
        new_sheet.write(top)
        new_sheet.write(f'{",".join(header)}\n')
        for sample in samples:
            new_sheet.write(f'{",".join(sample)}\n')
        # new_sheet.write(renderTemplate('SampleSheetv1_template.csv', data))

def getSampleSheet(lims, container_name, sample_sheet_path):
    """Get sample_sheet from clarity lims and write to sample_sheet_path."""
    for reagent_kit_artifact in lims.get_artifacts(containername=container_name):
        process = reagent_kit_artifact.parent_process
        for artifact in process.result_files():
            if artifact.name == 'SampleSheet csv' and artifact.files:
                file_id = artifact.files[0].id
                sample_sheet = lims.get_file_contents(id=file_id)
                sample_sheet_file = open(sample_sheet_path, 'w')
                sample_sheet_file.write(sample_sheet)
                return True
    return False



def parseConversionStats(dir):
    demux_stats = f'{dir}/Demultiplex_Stats.csv'
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
                    '# of >= Q30 Bases (PF)' : 0,
                    'Mean Quality Score (PF)' : 0
                }
            samples_tmp[ row['SampleID'] ]['Index'] = row['Index']
            samples_tmp[ row['SampleID'] ]['Lane'] = int(row['Lane'])
            samples_tmp[ row['SampleID'] ]['# Reads'] += int(row['# Reads'])
            samples_tmp[ row['SampleID'] ]['# Perfect Index Reads']  += int(row['# Perfect Index Reads'])
            samples_tmp[ row['SampleID'] ]['# One Mismatch Index Reads']  += int(row['# One Mismatch Index Reads'])
            samples_tmp[ row['SampleID'] ]['# of >= Q30 Bases (PF)']  += int(row['# of >= Q30 Bases (PF)'])
            samples_tmp[ row['SampleID'] ]['Mean Quality Score (PF)']  += float(row['Mean Quality Score (PF)'])
            # stats['samples'].append(row)
    for sampleID in samples_tmp:
        samples_tmp[sampleID]['# Reads'] = samples_tmp[sampleID]['# Reads'] / samples_tmp[ row['SampleID'] ]['Lane']
        samples_tmp[sampleID]['# Perfect Index Reads'] = samples_tmp[sampleID]['# Perfect Index Reads'] / samples_tmp[ row['SampleID'] ]['Lane']
        samples_tmp[sampleID]['# One Mismatch Index Reads'] = samples_tmp[sampleID]['# One Mismatch Index Reads'] / samples_tmp[ row['SampleID'] ]['Lane']
        samples_tmp[sampleID]['# of >= Q30 Bases (PF)'] = samples_tmp[sampleID]['# of >= Q30 Bases (PF)'] / samples_tmp[ row['SampleID'] ]['Lane']
        samples_tmp[sampleID]['Mean Quality Score (PF)'] = samples_tmp[sampleID]['Mean Quality Score (PF)'] / samples_tmp[ row['SampleID'] ]['Lane']
        stats['samples'].append(
            {'SampleID':sampleID,
            'Index' : samples_tmp[sampleID]['Index'],
            '# Reads' :  samples_tmp[sampleID]['# Reads'],
            '# Perfect Index Reads' : samples_tmp[sampleID]['# Perfect Index Reads'],
            '# One Mismatch Index Reads' : samples_tmp[sampleID]['# One Mismatch Index Reads'],
            '# of >= Q30 Bases (PF)' : samples_tmp[sampleID]['# of >= Q30 Bases (PF)'],
            'Mean Quality Score (PF)' : samples_tmp[sampleID]['Mean Quality Score (PF)'] })

    with open(top_unknown, 'r') as t:
        csv_reader = csv.DictReader(t)
        for row in islice(csv_reader,0,20):
            stats['top_unknown'].append(row)
    return stats

def statusMail(message, run_dir, projectIDs):
    status_file = Path(f'{run_dir}/Conversion/Logs/status.json')
    log_file = Path(f'{run_dir}/Conversion/Logs/mgr.log')
    error_file = Path(f'{run_dir}/Conversion/Logs/mgr.err')
    basepercent_by_cycle_plot = Path(f'{run_dir}/Conversion/Reports/{run_dir.name}_BasePercent-by-cycle_BasePercent.png')
    intensity_by_cycle_plot = Path(f'{run_dir}/Conversion/Reports/{run_dir.name}_Intensity-by-cycle_Intensity.png')
    clusterdensity_by_lane_plot = Path(f'{run_dir}/Conversion/Reports/{run_dir.name}_Clusters-by-lane.png')
    flowcell_intensity_plot = Path(f'{run_dir}/Conversion/Reports/{run_dir.name}_flowcell-Intensity.png')
    q_heatmap_plot = Path(f'{run_dir}/Conversion/Reports/{run_dir.name}_q-heat-map.png')
    q_histogram_plot = Path(f'{run_dir}/Conversion/Reports/{run_dir.name}_q-histogram.png')

    status = None
    log = None
    error = None

    with open(status_file, 'r') as s:
        status = json.loads(s.read() )
    with open(log_file, 'r') as l:
        log = l.read()
    with open(error_file, 'r') as e:
        error = e.read()

    expected_reads = getExpectedReads(f'{run_dir}/RunParameters.xml')
    conversion_stats = parseConversionStats(f'{run_dir}/Conversion/FastQ/Reports')

    #TODO ADD PROJECT ID TO SAMPLES

    attachments = {
        'zip_file': f'{run_dir}/{run_dir.name}_Reports.zip',
        'basepercent_by_cycle_plot': str(basepercent_by_cycle_plot) if basepercent_by_cycle_plot.is_file else None,
        'intensity_by_cycle_plot': str(intensity_by_cycle_plot) if intensity_by_cycle_plot.is_file else None,
        'clusterdensity_by_lane_plot': str(clusterdensity_by_lane_plot) if clusterdensity_by_lane_plot.is_file else None,
        'flowcell_intensity_plot': str(flowcell_intensity_plot) if flowcell_intensity_plot.is_file else None,
        'q_heatmap_plot': str(q_heatmap_plot) if q_heatmap_plot.is_file else None,
        'q_histogram_plot': str(q_histogram_plot) if q_histogram_plot.is_file else None,
    }
    template_data = {
        'status' : status,
        'log' : log,
        'error' : error,
        'projectIDs': ",".join(projectIDs),
        'run_dir': run_dir.name,
        'nr_reads' : f'{conversion_stats["total_reads"]:,} / {expected_reads:,}',
        'stats_summary': conversion_stats,
    }

    mail_content = renderTemplate('conversion_status_mail.html', template_data)
    mail_subject = f'[USEQ] Status ({",".join(projectIDs)}): {message}'
    sendMail(mail_subject,mail_content, MAIL_SENDER ,MAIL_ADMINS, attachments=attachments)

def updateLog(file,msg):
    with open(file, 'a') as f:
        f.write(f'{datetime.now()} : {msg}\n')
        print (f'{datetime.now()} : {msg}')


def updateStatus(file, status, step):
    status[step] = True
    with open(file, 'w') as f:
        f.write(json.dumps(status))


def demux_check(run_dir, log_file, error_file):
    Path(f'{run_dir}/Conversion/Demux-check').mkdir(parents=True, exist_ok=True)
    skip_demux = False

    sample_sheet = Path(f'{run_dir}/SampleSheet.csv')
    sample_sheet_parsed = parseSampleSheet( sample_sheet )
    sample_sheet_rev =  Path(f'{run_dir}/Conversion/Demux-check/SampleSheet-rev.csv')
    samples = sample_sheet_parsed['samples']
    rev_samples = []
    header = sample_sheet_parsed['header']

    if len(samples) > 1:
        # With 1 sample customers probably want the BCL files, demux will clear this up. For more samples cleanup samplesheet before demux
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
        writeSampleSheet(sample_sheet, header, samples, sample_sheet_parsed['top'])
        writeSampleSheet(sample_sheet_rev, header, rev_samples, sample_sheet_parsed['top'])


    #Samplesheet is OK first try
    updateLog(log_file, 'Checking original samplesheet : Running')
    command = f'{BCLCONVERT_PATH}/bcl-convert --bcl-input-directory {run_dir} --output-directory {run_dir}/Conversion/Demux-check/1 --sample-sheet {sample_sheet} --bcl-sampleproject-subdirectories true --force --first-tile-only true 1 > /dev/null'
    os.system(command)
    stats = parseConversionStats(f'{run_dir}/Conversion/Demux-check/1/Reports')
    if stats['undetermined_reads'] / stats['total_reads'] < 0.25:
        updateLog(log_file, 'Checking original samplesheet : Done')
        return True

    #Try revcomp samplesheet
    command = f'{BCLCONVERT_PATH}/bcl-convert --bcl-input-directory {run_dir} --output-directory {run_dir}/Conversion/Demux-check/2 --sample-sheet {sample_sheet_rev} --bcl-sampleproject-subdirectories true --force --first-tile-only true 1 > /dev/null'
    os.system(command)
    stats = parseConversionStats(f'{run_dir}/Conversion/Demux-check/2/Reports')
    if stats['undetermined_reads'] / stats['total_reads'] < 0.25:
        updateLog(log_file, 'Note : Reverse complemented index2')
        updateLog(log_file, 'Checking original samplesheet : Done')

        # return (True, sample_sheet_rev)
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

    # for sample in samples:
    # print(sample_names)
def uploadToNextcloud(lims, run_dir, mode,projectIDs,log_file, error_file):
    machine = run_dir.parents[0].name

    #Create .tar files for upload to nextcloud
    if mode == 'fastq':
        for pid in projectIDs:
            pid_staging = Path(f'{STAGING_DIR}/{pid}')
            pid_staging.mkdir(parents=True, exist_ok=True)


            pid_samples  = set()
            pid_dir = Path(f'{run_dir}/Conversion/FastQ/{pid}')
            for fastq in pid_dir.glob('*.fastq.gz'):
                name = fastq.name.split('_')[0]
                pid_samples.add(name)

            for sample in pid_samples:
                sample_zip = Path(f'{pid_staging}/{sample}.tar')
                sample_zip_done = Path(f'{pid_staging}/{sample}.tar.done')
                if not sample_zip_done.is_file():
                    zip_command = f'cd {pid_dir} && tar -cf {sample_zip} {sample}*fastq.gz 1>> {log_file} 2>> {error_file}'
                    updateLog(log_file, f'Compressing {pid} {sample} to tar : Running')
                    exit_code = os.system(zip_command)
                    if exit_code: return False
                    updateLog(log_file, f'Compressing {pid} {sample} to tar : Done')
                    sample_zip_done.touch()


    else:
        pid = projectIDs[0]
        pid_staging = Path(f'{STAGING_DIR}/{pid}')
        pid_staging.mkdir(parents=True, exist_ok=True)

        zipped_run = Path(f'{pid_staging}/{pid}.tar')
        zip_done = Path(f'{pid_staging}/{pid}.tar.done')

        zip_command = f'cd {run_dir.parents[0]} && tar -cf {zipped_run} --exclude "Conversion/" --exclude "*jpg" --exclude "*fastq.gz*" --exclude "*.filter" --exclude "*tif" --exclude "*run_zip.*" {run_dir.name} 1>> {log_file} 2>> {error_file}'
        if not zip_done.is_file():
            updateLog(log_file, f'Compressing {pid} to tar : Running')
            # print(zip_command)
            exit_code = os.system(zip_command)
            if exit_code: return False
            updateLog(log_file, f'Compressing {pid} to tar : Done')
            zip_done.touch()

    #Filter stats files per PID
    for pid in projectIDs:
        pid_staging = Path(f'{STAGING_DIR}/{pid}')
        report_dir = Path(f'{run_dir}/Conversion/FastQ/Reports')
        filterStats(lims, pid, pid_staging, report_dir)

    #Create md5sums for .tar files
    for pid in projectIDs:
        pid_staging = Path(f'{STAGING_DIR}/{pid}')
        updateLog(log_file, f'Creating md5sums for {pid} : Running')
        md5_command = f'md5sum {pid_staging}/*.tar > {pid_staging}/md5sums.txt'
        exit_code = os.system(md5_command)
        updateLog(log_file, f'Creating md5sums for {pid} : Done')


    #Upload .tar/stats & md5sums to nextcloud
    for pid in projectIDs:
        pid_staging = Path(f'{STAGING_DIR}/{pid}')
        pid_done = Path(f'{STAGING_DIR}/{pid}/transfer_done')

        if nextcloud_util.checkExists(pid):
            for file in pid_staging.iterdir():
                 if nextcloud_util.checkExists(f'{pid}/{file.name}'):
                     nextcloud_util.delete(f'{pid}/{file.name}')
            nextcloud_util.delete(pid)

        transfer_command = "rsync -rah --include '*.tar' --include 'md5sums.txt' --include '*.csv' --exclude '*' "
        transfer_command += f'{pid_staging} {NEXTCLOUD_HOST}:{NEXTCLOUD_DATA_ROOT}/{NEXTCLOUD_RAW_DIR}/ 1>> {log_file} 2>> {error_file}'
        updateLog(log_file, f'Transferring {pid} to nextcloud : Running')
        exit_code = os.system(transfer_command)
        if exit_code: return False
        pid_done.touch()
        done_command = f"scp {pid_done} {NEXTCLOUD_HOST}:{NEXTCLOUD_DATA_ROOT}/{NEXTCLOUD_RAW_DIR}/{pid}"
        exit_code = os.system(done_command)
        updateLog(log_file, f'Transferring {pid} to nextcloud : Done')

    for file in pid_staging.iterdir():
        file.unlink()
    return True

def uploadToHPC(lims, run_dir, projectIDs, error_file, log_file):
    machine = run_dir.parents[0].name
    to_sync = ''
    rsync_command = '/usr/bin/rsync -rah --update --stats --verbose --prune-empty-dirs '
    for pid in projectIDs:
        project = Project(lims, id=pid)
        project_name = project.name

        samples = lims.get_samples(projectlimsid=project.id)
        analysis_steps = samples[0].udf['Analysis'].split(',')
        if len(analysis_steps) > 1:
            rsync_command += f'--include "Conversion/FastQ/{pid}/*.fastq.gz" '
        else:
            rsync_command += f'--exclude "Conversion/FastQ/{pid}/*.fastq.gz" '

    rsync_command += " --include '*/' --include 'md5sum.txt' --include 'SampleSheet.csv' --include 'RunInfo.xml' --include '*unParameters.xml' --include 'InterOp/**' --include '*/*/Reports/**' --include 'Data/Intensities/BaseCalls/Stats/**' --include '*.[pP][eE][dD]'"
    rsync_command += " --exclude '*'"
    rsync_command += f" {run_dir}"
    rsync_command += f" {DATA_DIR_HPC}/{machine} 1> /dev/null 2>> {error_file}"

    updateLog(log_file, 'Upload to HPC : Running')
    # print (rsync_command)
    exit_code = os.system(rsync_command)
    if exit_code:
        return False
    updateLog(log_file, 'Upload to HPC : Done')
    return True

def uploadToArchive(run_dir, error_file, log_file):
    updateLog(log_file, "Upload to archive : Running")
    machine = run_dir.parents[0].name
    rsync_command = f"rsync -rahm --exclude '*jpg' --exclude '*fastq.gz' --exclude '*fq.gz' {run_dir} {ARCHIVE_DIR}/{machine} 1> /dev/null 2>> {error_file}"
    exit_code = os.system(rsync_command)
    if exit_code:
        return False

    updateLog(log_file, "Upload to archive : Done")
    return True


def cleanup(run_dir, error_file, log_file):
    updateLog(log_file, "Cleaning up : Running")
    for file in run_dir.glob("**/*.gz"):
        if file.name.endswith(".fastq.gz") or file.name.endswith(".fq.gz"):
            file.unlink()
    updateLog(log_file, "Cleaning up : Done")

def manageRuns(lims):
    for machine_dir in DATA_DIRS_RAW:

        md_path = Path(machine_dir)
        for run_dir in md_path.glob("*"):

            if run_dir.name.count('_') != 3 or not run_dir.is_dir(): continue #Not a valid run directory
            #Important Files
            sample_sheet = Path(f'{run_dir}/SampleSheet.csv')
            rta_complete = Path(f'{run_dir}/RTAComplete.txt')
            running_file = Path(f'{run_dir}/.mgr_running')
            failed_file = Path(f'{run_dir}/.mgr_failed')
            done_file = Path(f'{run_dir}/.mgr_done')
            status_file = Path(f'{run_dir}/Conversion/Logs/status.json')
            log_file = Path(f'{run_dir}/Conversion/Logs/mgr.log')
            error_file = Path(f'{run_dir}/Conversion/Logs/mgr.err')
            if rta_complete.is_file() and not (running_file.is_file() or failed_file.is_file() or done_file.is_file()): #Run is done and not being processed/has failed/is done
                #Lock directory
                running_file.touch()

                status = {
                    'Demux-check' : False,
                    'Conversion' : False,
                    'Transfer-nc' : False,
                    'Transfer-hpc' : False,
                    'Archive' : False,
                }

                try:
                    #Set up important directories
                    Path(f'{run_dir}/Conversion/Logs').mkdir(parents=True, exist_ok=True)
                    Path(f'{run_dir}/Conversion/Reports').mkdir(parents=True, exist_ok=True)
                    Path(f'{run_dir}/Conversion/FastQ').mkdir(parents=True, exist_ok=True)

                    #Check/set status
                    if status_file.is_file():
                        with open(status_file, 'r') as s:
                            status = json.loads( s.read() )

                    #Retrieve various metadata
                    updateLog(log_file,'Retrieving data from RunParameters.xml')
                    flowcell = run_dir.name.split("_")[-1]
                    run_parameters_old = Path(f'{run_dir}/runParameters.xml')
                    run_parameters = Path(f'{run_dir}/RunParameters.xml')
                    if run_parameters_old.is_file():
                        run_parameters_old.rename(run_parameters)

                    run_parameters = xml.dom.minidom.parse(f'{run_dir}/RunParameters.xml')
                    experiment_name = run_parameters.getElementsByTagName('ExperimentName')[0].firstChild.nodeValue

                    if '_' in experiment_name: #Novaseq exception
                        experiment_name = experiment_name.split("_")[3]
                    experiment_name = experiment_name.replace('REDO','')


                    # project = Project(lims, id=experiment_name)
                    # project_name = project.name

                    if run_parameters.getElementsByTagName('ReagentKitSerial'):  # NextSeq
                        lims_container_name = run_parameters.getElementsByTagName('ReagentKitSerial')[0].firstChild.nodeValue
                    elif run_parameters.getElementsByTagName('LibraryTubeSerialBarcode'):  # NovaSeq
                        lims_container_name = run_parameters.getElementsByTagName('LibraryTubeSerialBarcode')[0].firstChild.nodeValue


                    if not sample_sheet.is_file():
                        updateLog(log_file,'No SampleSheet.csv found.')
                        if [x for x in run_dir.glob("*csv")][0]: #Check if samplesheet with different name exists
                            s = [x for x in run_dir.glob("*csv")][0]
                            updateLog(log_file,f'Found {s} , renaming to SampleSheet.csv.')
                            s.rename(f'{run_dir}/SampleSheet.csv')
                        else: #No samplesheet found, try to find it in LIMS
                            updateLog(log_file,f'Tring to find SampleSheet.csv in LIMS.')
                            getSampleSheet(lims, lims_container_name, sample_sheet)


                    if status['Demux-check'] == False:
                        status['Demux-check']  = demux_check( run_dir, log_file, error_file )
                        updateStatus(status_file, status, 'Demux-check')

                    projectIDs = set()
                    sheet = parseSampleSheet(sample_sheet)
                    for sample in sheet['samples']:
                        projectIDs.add( sample[ sheet['header'].index('Sample_Project') ] )

                    if status['Demux-check']: #Demultiplexing will probably succeed, so carry on
                        updateLog(log_file,f'Pre demultiplexing check was succesful.')

                        if not status['Conversion']:
                            updateLog(log_file,f'Starting demultiplexing.')
                            exit_code = convertBCL(run_dir, sample_sheet, log_file, error_file)
                            if exit_code == 0:  # Conversion completed
                                addFlowcellToFastq(run_dir, flowcell,log_file, error_file)

                                md5sumFastq(run_dir,log_file, error_file)

                                zip_file = zipConversionReport(run_dir,log_file, error_file)

                                if sum(generateRunStats(run_dir, log_file, error_file)) > 0:
                                    raise RuntimeError(f'Demultiplexing probably failed, failed to create conversion statistics. If this is ok please set Conversion to True in {status_file} and remove {failed_file}.',run_dir, experiment_name, project_name)
                                else:
                                    updateStatus(status_file, status, 'Conversion')
                            else:
                                raise RuntimeError('Demultiplexing failed with unknown error.',run_dir, projectIDs)

                        if status['Conversion']: #Conversion succesful,
                            if not status['Transfer-nc']:
                                status['Transfer-nc'] = uploadToNextcloud(lims,run_dir, 'fastq',projectIDs,log_file, error_file)
                            if status['Transfer-nc'] :
                                updateStatus(status_file, status, 'Transfer-nc')
                            else:
                                raise RuntimeError('Transfer to nextcloud failed.',run_dir, projectIDs)

                            if not status['Transfer-hpc']:
                                status['Transfer-hpc'] = uploadToHPC(lims, run_dir, projectIDs,error_file, log_file)
                            if status['Transfer-hpc']:
                                updateStatus(status_file, status, 'Transfer-hpc')
                            else:
                                raise RuntimeError('Transfer to hpc failed.',run_dir, projectIDs)

                    else: #Skip straight to transfer
                        updateLog(log_file,f'Pre demultiplexing check failed, skipping demultiplexing.')
                        if not status['Transfer-nc']:
                            status['Transfer-nc'] = uploadToNextcloud(run_dir, 'fastq',projectIDs,log_file, error_file)
                        if status['Transfer-nc'] :
                            updateStatus(status_file, status, 'Transfer-nc')
                            # cleanup(run_dir)
                        else:
                            raise RuntimeError('Transfer to nextcloud failed.',run_dir, projectIDs)

                        if not status['Transfer-hpc']:
                            status['Transfer-hpc'] = uploadToHPC(lims, run_dir, projectIDs,error_file, log_file)
                        if status['Transfer-hpc']:
                            updateStatus(status_file, status, 'Transfer-hpc')
                        else:
                            raise RuntimeError('Transfer to hpc failed.',run_dir, projectIDs)

                    if not status['Archive']:
                        status['Archive'] = uploadToArchive(run_dir, error_file, log_file)
                    if status['Archive']:
                        updateStatus(status_file, status, 'Archive')
                    else:
                        raise RuntimeError('Transfer to archive storage failed.',run_dir, projectIDs)

                    if status['Conversion'] and status['Transfer-nc'] and status['Transfer-hpc'] and status['Archive']:
                        # cleanup(run_dir, error_file, log_file)
                        statusMail('Processing finished', run_dir,projectIDs)
                        running_file.unlink()
                        done_file.touch()

                except RuntimeError as e:
                    print(e)
                    statusMail(e.args[0],e.args[1],e.args[2])
                    running_file.unlink()
                    failed_file.touch()



def run(lims):
    """Runs the manageRuns function"""
    global nextcloud_util
    #Set up nextcloud
    nextcloud_util = NextcloudUtil()
    nextcloud_util.setHostname( NEXTCLOUD_HOST )
    nextcloud_util.setup( NEXTCLOUD_USER, NEXTCLOUD_PW, NEXTCLOUD_WEBDAV_ROOT,NEXTCLOUD_RAW_DIR,MAIL_SENDER )


    manageRuns(lims )
