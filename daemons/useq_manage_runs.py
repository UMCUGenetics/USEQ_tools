import os
import argparse
import subprocess
# import logging
# import logging.handlers
import xml.dom.minidom
import re
import json
import csv
import datetime
from config import DATA_DIRS_RAW,DATA_DIR_HPC,ARCHIVE_DIR,MAIL_SENDER,MAIL_ADMINS,INTEROP_PATH,RUNTYPE_YIELDS,BCLCONVERT_PATH,BCLCONVERT_PROCESSING_THREADS,BCLCONVERT_WRITING_THREADS,STAGING_DIR,NEXTCLOUD_DATA_ROOT,NEXTCLOUD_PW,NEXTCLOUD_USER,NEXTCLOUD_HOST,NEXTCLOUD_RAW_DIR
from modules.useq_mail import sendMail
from modules.useq_illumina_parsers import parseConversionStats, getExpectedReads,parseSampleSheet
from modules.useq_nextcloud import NextcloudUtil
from pathlib import Path
from modules.useq_template import TEMPLATE_PATH,TEMPLATE_ENVIRONMENT,renderTemplate
from datetime import datetime
from webdav3.client import Client
from genologics.entities import Project

def revcomp(seq):
    revcompl = lambda x: ''.join([{'A':'T','C':'G','G':'C','T':'A'}[B] for B in x][::-1])
    return revcompl(seq)

def convertBCL(run_dir, sample_sheet, log_file, error_file):
    """Convert bcl files from run to fastq.gz files."""

    # Start conversion
    os.system(f'date >> {log_file}')
    command = f'{BCLCONVERT_PATH}/bcl-convert --bcl-input-directory {run_dir} --output-directory {run_dir}/Conversion/FastQ --bclsampleprojectsubdirectories true --force --sample-sheet {sample_sheet}'
    command = f'{command} 1>> {log_file} 2>> {error_file}'
    exit_code = os.system(command)
    return exit_code
#
def addFlowcellToFastq(run_dir, flowcell_id):
    """Add flowcell id to fastq.gz filename."""
    base_calls_dir = Path(f'{run_dir}/Conversion/FastQ')
    for fastq in base_calls_dir.rglob("*.fastq.gz"):
        filename_parts = fastq.name.split('_')
        if filename_parts[1] != flowcell_id:
            filename_parts.insert(1, flowcell_id)
            new_filename = '_'.join(filename_parts)
            fastq.rename(f'{fastq.parent}/{new_filename}')


def zipConversionReport(run_dir):
    """Zip conversion reports."""
    zip_file = f'{run_dir}/{run_dir.name}_Reports.zip'
    os.chdir(f'{run_dir}/Conversion/')
    os.system(f'zip -FSr {zip_file} Reports/')
    return zip_file
#
def md5sumFastq(run_dir):
    """Generate md5sums for all fastq.gz files from a sequencing run."""
    command = f'(cd {run_dir}/Conversion/FastQ && find . -type f -iname "*.fastq.gz" -exec md5sum {{}} \\; > md5sum.txt)'
    os.system(command)

def generateRunStats(run_dir):
    """Create run stats files using interop tool."""
    stats_dir = Path(f'{run_dir}/Conversion/Reports')

    exit_codes = []
    os.chdir(stats_dir)

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
    exit_codes.append(os.system(f'multiqc {run_dir} -o {stats_dir} -k json'))
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


def conversionFailedMail(run_dir, experiment_name, project_name):

    ce = open( f'{run_dir}/Conversion/Logs/conversion_error.txt', 'r')
    cl = open( f'{run_dir}/Conversion/Logs/conversion_log.txt', 'r')
    template_data = {
        'experiment_name' : experiment_name,
        'run_dir' : run_dir,
        'conversion_error' : ce.read(),
        'conversion_log' : cl.read()
    }
    mail_content = renderTemplate('conversion_failed_template.html', template_data)
    mail_subject = f'ERROR: Conversion of {experiment_name} - {project_name} failed!'
    sendMail(mail_subject,mail_content, MAIL_SENDER ,MAIL_ADMINS)
    ce.close()
    cl.close()
#
def conversionSuccesMail(run_dir, experiment_name, project_name):
    machine = run_dir.parents[0].name
    subject = f'Conversion of {experiment_name} - {project_name} finished'
    attachments = None
    template_data = None
    content = None
    expected_reads = parseRunParameters( f'{run_dir}/RunParameters.xml' )
    error = None

    # FIX THIS!!

    # try:
    #     conversion_stats = parseConversionStats( f'{run_dir}/Data/Intensities/BaseCalls/Stats/ConversionStats.xml' )
    # except:
    #     error = f'Failed to find ConversionStats.xml for {experiment_name}'

    if not error:
        attachments = {
            'zip_file': f'{run_dir}/{run_dir.name}_Reports.zip',
            'basepercent_by_cycle_plot': f'{run_dir}/Conversion/Reports/{run_dir.name}_BasePercent-by-cycle_BasePercent.png',
            'intensity_by_cycle_plot': f'{run_dir}/Conversion/Reports/{run_dir.name}_Intensity-by-cycle_Intensity.png',
            'clusterdensity_by_lane_plot': f'{run_dir}/Conversion/Reports/{run_dir.name}_Clusters-by-lane.png',
            'flowcell_intensity_plot': f'{run_dir}/Conversion/Reports/{run_dir.name}_flowcell-Intensity.png',
            'q_heatmap_plot': f'{run_dir}/Conversion/Reports/{run_dir.name}_q-heat-map.png',
            'q_histogram_plot': f'{run_dir}/Conversion/Reports/{run_dir.name}_q-histogram.png',
        }
        template_data = {
            'experiment_name': experiment_name,
            'run_dir': run_dir.name,
            'rsync_location': f" {machine}",
            'nr_reads': f'{conversion_stats["total_reads"]:,} / {conversion_stats["total_reads_raw"]:,} / {expected_reads:,}',
            'stats_summary': conversion_stats,
            'error' : error
        }
    else:
        template_data = {
            'experiment_name': experiment_name,
            'run_dir': run_dir.name,
            'rsync_location': f" {machine}",
            'nr_reads': None,
            'stats_summary': None,
            'error' : error
        }

    mail_content = renderTemplate('conversion_done_template.html', template_data)

    mail_subject = f'Conversion of {experiment_name} - {project_name} finished'
    sendMail(mail_subject,mail_content, MAIL_SENDER ,MAIL_ADMINS, attachments=attachments)

def transferFailedMail(run_dir, experiment_name, project_name):

    transfer_log = Path(f'{run_dir}/Conversion/Logs/transfer.log')
    transfer_error = Path(f'{run_dir}/Conversion/Logs/transfer.err')
    te = transfer_error.open()
    tl = transfer_log.open()

    template_data = {
        'experiment_name' : experiment_name,
        'run_dir' : run_dir,
        'transfer_error' : te.read(),
        'transfer_log' : tl.read()
    }
    mail_content = renderTemplate('transfer_failed_template.html', template_data)
    mail_subject = f'ERROR: Transfer of {experiment_name} - {project_name} failed!'
    sendMail(mail_subject,mail_content, MAIL_SENDER ,MAIL_ADMINS)

def archiveFailedMail(run_dir, experiment_name, project_name):
    archive_error = Path(f'{run_dir}/Conversion/Logs/illumina_archive.err')
    archive_log = Path(f'{run_dir}/Conversion/Logs/illumina_archive.log')
    ae = archive_error.open()
    al = archive_log.open()

    template_data = {
        'experiment_name' : experiment_name,
        'run_dir' : run_dir,
        'archive_error' : ae.read(),
        'archive_log' : al.read()
    }
    mail_content = renderTemplate('transfer_failed_template.html', template_data)
    mail_subject = f'ERROR: Transfer of {experiment_name} - {project_name} failed!'
    sendMail(mail_subject,mail_content, MAIL_SENDER ,MAIL_ADMINS)

def parseConversionStats(dir):
    demux_stats = f'{dir}/Reports/Demultiplex_Stats.csv'
    stats = {
        'total_reads' : 0,
        'undetermined_reads' : 0,
        'samples' : []
    }
    with open(demux_stats, 'r') as d:
        csv_reader = csv.DictReader(d)
        for row in csv_reader:
            if row['SampleID'] == 'Undetermined':
                stats['undetermined_reads'] += float(row['# Reads'])
            else:
                stats['total_reads'] += float(row['# Reads'])
            stats['samples'].append(row)

    return stats

def statusMail(message, run_dir, experiment_name, project_name):
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
    conversion_stats = parseConversionStats(run_dir)

    attachments = {
        'zip_file': f'{run_dir}/{run_dir.name}_Reports.zip',
        'basepercent_by_cycle_plot': basepercent_by_cycle_plot if basepercent_by_cycle_plot.is_file else None,
        'intensity_by_cycle_plot': intensity_by_cycle_plot if intensity_by_cycle_plot.is_file else None,
        'clusterdensity_by_lane_plot': clusterdensity_by_lane_plot if clusterdensity_by_lane_plot.is_file else None,
        'flowcell_intensity_plot': flowcell_intensity_plot if flowcell_intensity_plot.is_file else None,
        'q_heatmap_plot': q_heatmap_plot if q_heatmap_plot.is_file else None,
        'q_histogram_plot': q_histogram_plot if q_histogram_plot.is_file else None,
    }
    template_data = {
        'status' : status,
        'log' : log,
        'error' : error,
        'experiment_name': experiment_name,
        'run_dir': run_dir.name,
        'nr_reads' : f'{conversion_stats["total_reads"]:,} / {expected_reads:,}',
        'stats_summary': conversion_stats,
    }

    mail_content = renderTemplate('conversion_status_mail.html', template_data)
    mail_subject = f'[USEQ] Status ({experiment_name}-{project_name}): {message}'
    sendMail(mail_subject,mail_content, MAIL_SENDER ,MAIL_ADMINS, attachments=attachments)

def updateLog(file,msg):
    with open(file, 'wa') as f:
        f.write(f'{datetime.datetime.now()} : {msg}')
        print (f'{datetime.datetime.now()} : {msg}')


def updateStatus(file, status, step):
    status[step] = True
    with open(file, 'w') as f:
        f.write(json.dumps(status))


def demux_check(run_dir):
    Path(f'{run_dir}/Conversion/tmp').mkdir(parents=True, exist_ok=True)
    skip_demux = False

    sample_sheet = parseSampleSheet( Path(f'{run_dir}/SampleSheet.csv') )
    sample_sheet_rev = parseSampleSheet( Path(f'{run_dir}/SampleSheet-rev.csv') )
    samples = sample_sheet['samples']
    rev_samples = []
    header = sample_sheet['header']

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
                revseq = revcomp(sample_rev[header.index('index1')])
                sample_rev[header.index('index1')] = revseq
            rev_samples.append(sample_rev)
        writeSampleSheet(sample_sheet, header, samples, sample_sheet['top'])
        writeSampleSheet(sample_sheet_rev, header, rev_samples, sample_sheet['top'])


    #Samplesheet is OK first try
    command = f'{BCLCONVERT_PATH}/bcl-convert --bcl-input-directory {run_dir} --output-directory {run_dir}/Conversion/tmp --sample-sheet {sample_sheet} --bclsampleprojectsubdirectories true --force --first-tile-only true'
    os.system(command)
    stats = parseConversionStats(f'{run_dir}/Conversion/tmp')
    if stats['undetermined_reads'] / stats['total_reads'] > 0.75:
        return (True, sample_sheet)

    #Try revcomp samplesheet
    command = f'{BCLCONVERT_PATH}/bcl-convert --bcl-input-directory {run_dir} --output-directory {run_dir}/Conversion/tmp --sample-sheet {sample_sheet_rev} --bclsampleprojectsubdirectories true --force --first-tile-only true'
    os.system(command)
    stats = parseConversionStats(f'{run_dir}/Conversion/tmp')
    if stats['undetermined_reads'] / stats['total_reads'] > 0.75:
        return (True, sample_sheet_rev)

    return (False, '')




def uploadToNextcloud(run_dir, mode,experiment_name,log_file, error_file):
    machine = run_dir.parents[0].name
    zipped_run = Path(f'{STAGING_DIR}/{experiment_name}-raw.tar')
    zipped_run_md5 = Path(f'{STAGING_DIR}/{experiment_name}-raw.tar.md5')
    zip_done = Path(f'{STAGING_DIR}/{experiment_name}-raw.tar.done')
    zip_command = None


    if mode == 'fastq':
        zip_command = f'cd {run_dir.parents[0]} && tar -cf {zipped_run} --exclude "*jpg" --exclude "*bcl*" --exclude "*.filter" --exclude "*tif" --exclude "*run_zip.*" {run_dir.name} 1>> {log_file} 2>> {error_file} && md5sum {zipped_run} > {zipped_run_md5}'
    else:
        zip_command = f'cd {run_dir.parents[0]} && tar -cf {zipped_run} --exclude "*jpg" --exclude "*fastq.gz*" --exclude "*.filter" --exclude "*tif" --exclude "*run_zip.*" {run_dir.name} 1>> {log_file} 2>> {error_file} && md5sum {zipped_run} > {zipped_run_md5}'

    updateLog(log_file, 'Compressing run to tar.')

    if not zip_done.is_file():
        exit_code = os.system(zip_command)
        if not exit_code:
            zip_done.touch()
        else:
            return False

    if nextcloud_util.checkExists( f'{experiment_name}-raw.tar' ):
        nextcloud_util.delete(f'{experiment_name}-raw.tar')
    updateLog(log_file, 'Transferring run to nextcloud.')
    transfer_command = f'scp {zipped_run} {NEXTCLOUD_HOST}:{NEXTCLOUD_DATA_ROOT}/{NEXTCLOUD_RAW_DIR} 1>> {log_file} 2>> {error_file}'
    exit_code = os.system(transfer_command)
    if exit_code:
        return False

    zipped_run.unlink()
    zip_done.unlink()
    return True

def uploadToHPC(lims, run_dir, experiment_name, project_name, error_file, log_file)
    project = Project(lims, id=experiment_name)
    project_name = project.name
    samples = lims.get_samples(projectlimsid=project.id)
    analysis_steps = samples[0].udf['Analysis'].split(',')

    rsync_command = '/usr/bin/rsync -rah --update --stats --verbose --prune-empty-dirs'
    if len(analysis_steps) > 1:
        rsync_command += " --include '*.fq.gz' --include '*.fastq.gz'"
    rsync_command += " --include '*/' --include 'md5sum.txt' --include 'SampleSheet.csv' --include 'RunInfo.xml' --include '*unParameters.xml' --include 'InterOp/**' --include '*/*/Reports/**' --include 'Data/Intensities/BaseCalls/Stats/**' --include '*.[pP][eE][dD]'"
    rsync_command += " --exclude '*'"
    rsync_command += f" {run_dir}"
    rsync_command += f" {DATA_DIR_HPC}/{machine} 1>> {log_file} 2>> {error_file}"

    updateLog(log_file, 'Running upload to HPC.')
    exit_code = os.system(rsync_command)
    if exit_code:
        return False

    return True

def uploadToArchive(run_dir, error_file, log_file):

    machine = run_dir.parents[0].name
    rsync_command = f"rsync -rahm --exclude '*jpg' --exclude '*fastq.gz' --exclude '*fq.gz' {run_dir} {ARCHIVE_DIR}/{machine} 1>> {log_file} 2>> {error_file}"
    exit_code = os.system(rsync_command)
    if exit_code:
        return False

    return True


def cleanup(run_dir):
    for file in run_dir.glob("**/*.gz"):
        if file.name.endswith(".fastq.gz") or file.name.endswith(".fq.gz"):
            file.unlink()

def manageRuns(lims):
    #
    # smtp_handler = logging.handlers.SMTPHandler(
    #     mailhost=("smtp-open.umcutrecht.nl", 25),
    #     fromaddr=MAIL_SENDER,
    #     toaddrs=MAIL_ADMINS,
    #     subject=u"Error occured during conversion!")
    #
    # logger = logging.getLogger()
    # logger.addHandler(smtp_handler)




    for machine_dir in DATA_DIRS_RAW:
        #print(machine_dir)
        md_path = Path(machine_dir)
        for run_dir in md_path.glob("*"):
            if run_dir.name.count('_') != 3 or not run_dir.is_dir(): continue #Not a valid run directory
            #Important Files
            sample_sheet = None
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

                    updateLog(log_file,f'Running pre demultiplexing check.')
                    if status['Demux-check'] == False:
                        status['Demux-check'], sample_sheet = demux_check( run_dir )
                        updateStatus(status_file, status, 'Demux-check')


                    if status['Demux-check']: #Demultiplexing will probably succeed, so carry on
                        updateLog(log_file,f'Pre demultiplexing check was succesful.')

                        if not status['Conversion']:
                            updateLog(log_file,f'Starting demultiplexing.')
                            exit_code = convertBCL(run_dir, sample_sheet, log_file, error_file)
                            if exit_code == 0:  # Conversion completed
                                updateLog(log_file,f'Demultiplexing completed.')

                                updateLog(log_file,f'Adding flowcell to FastQ file names.')
                                addFlowcellToFastq(run_dir, flowcell)

                                updateLog(log_file,f'Generating md5sums for FastQ files.')
                                md5sumFastq(run_dir)

                                updateLog(log_file,f'Zipping conversion reports.')
                                zip_file = zipConversionReport(run_dir)

                                updateLog(log_file,f'Generating run plots.')
                                if sum(generateRunStats(run_dir)) > 0:
                                    raise RuntimeError(f'Demultiplexing probably failed, failed to create conversion statistics. If this is ok please set Conversion to True in {status_file} and remove {failed_file}.',run_dir, experiment_name, project_name)
                                    # updateLog(error_file, f'Demultiplexing probably failed, failed to create conversion statistics. If this is ok please set Conversion to True in {status_file} and remove {failed_file}.')

                                    # failed_file.touch()
                                    # running_file.unlink()
                                    # statusMail(status,run_dir, experiment_name, project_name)
                                else:
                                    updateLog(log_file,f'Demultiplexing done.')
                                    updateStatus(status_file, status, 'Conversion')
                            else:
                                raise RuntimeError('Demultiplexing failed with unknown error.',run_dir, experiment_name, project_name)
                                # updateLog(error_file,f)
                                # statusMail(status,run_dir, experiment_name, project_name)

                        if status['Conversion']: #Conversion succesful,
                            if not status['Transfer-nc']:
                                status['Transfer-nc'] = uploadToNextcloud(run_dir, 'fastq',experiment_name,log_file, error_file):
                            if status['Transfer-nc'] :
                                updateStatus(status_file, status, 'Transfer-nc')
                            else:
                                raise RuntimeError('Transfer to nextcloud failed.',run_dir, experiment_name, project_name)
                                # updateLog(error_file,f'Transfer to nextcloud failed.')
                                # statusMail(status,run_dir, experiment_name, project_name)

                            if not status['Transfer-hpc']:
                                status['Transfer-hpc'] = uploadToHPC(lims, run_dir, experiment_name, project_name)
                            if status['Transfer-hpc']:
                                updateStatus(status_file, status, 'Transfer-hpc')
                            else:
                                raise RuntimeError('Transfer to hpc failed.',run_dir, experiment_name, project_name)
                                # updateLog(error_file,f'Transfer to hpc failed.')
                                # statusMail(status,run_dir, experiment_name, project_name)

                    else: #Skip straight to transfer
                        updateLog(log_file,f'Pre demultiplexing check failed, skipping demultiplexing.')
                        if not status['Transfer-nc']:
                            status['Transfer-nc'] = uploadToNextcloud(run_dir, 'fastq',experiment_name,log_file, error_file):
                        if status['Transfer-nc'] :
                            updateStatus(status_file, status, 'Transfer-nc')
                            # cleanup(run_dir)
                        else:
                            raise RuntimeError('Transfer to nextcloud failed.',run_dir, experiment_name, project_name)
                            # updateLog(error_file,f'Transfer to nextcloud failed.')
                            # statusMail(status,run_dir, experiment_name, project_name)

                        if not status['Transfer-hpc']:
                            status['Transfer-hpc'] = uploadToHPC(lims, run_dir, experiment_name, project_name)
                        if status['Transfer-hpc']:
                            updateStatus(status_file, status, 'Transfer-hpc')
                        else:
                            raise RuntimeError('Transfer to hpc failed.',run_dir, experiment_name, project_name)
                            # updateLog(error_file,f'Transfer to hpc failed.')
                            # statusMail(status,run_dir, experiment_name, project_name)

                    if not status['Archive']:
                        status['Archive'] = uploadToArchive()
                    if status['Achive']:
                        updateStatus(status_file, status, 'Archive')
                    else:
                        raise RuntimeError('Transfer to archive storage failed.',run_dir, experiment_name, project_name)
                        # updateLog(error_file, f'Transfer to archive storage failed.')
                        # statusMail(status,run_dir, experiment_name, project_name)

                    if status['Conversion'] and status['Transfer-nc'] and status['Transfer-hpc'] and status['Archive']:
                        cleanup(run_dir)
                        statusMail('Processing finished', run_dir, experiment_name, project_name)
                        running_file.unlink()
                        done_file.touch()

                except RuntimeError as e:
                    statusMail(e.args[0],e.args[1],e.args[2],e.args[3])
                    running_file.unlink()
                    failed_file.touch()

                # finally:
                #     running_file.unlink() #Whatever happens, clear running_file on exit
                    # stats_dir = Path(f'{run_dir}/Conversion/Reports')
                    #
                    # if not stats_dir.is_dir():
                    #     # Create 'Stats' dir
                    #     stats_dir.mkdir()

            # conversion_done = Path(f'{run_dir}/Conversion/Logs/ConversionDone.txt')
            # conversion_running = Path(f'{run_dir}/Conversion/Logs/ConversionRunning.txt')
            # conversion_failed = Path(f'{run_dir}/Conversion/Logs//ConversionFailed.txt')
            # transfer_running = Path(f'{run_dir}/Conversion/Logs/TransferRunning.txt')
            # transfer_done = Path(f'{run_dir}/Conversion/Logs/TransferDone.txt')
            # transfer_failed = Path(f'{run_dir}/Conversion/Logs/TransferFailed.txt')
            # archive_done = Path(f'{run_dir}/Conversion/Logs/ArchiveDone.txt')
            # archive_running = Path(f'{run_dir}/Conversion/Logs/ArchiveRunning.txt')
            # archive_failed = Path(f'{run_dir}/Conversion/Logs/ArchiveFailed.txt')
            # run_parameters_old = Path(f'{run_dir}/runParameters.xml')
            # run_parameters = Path(f'{run_dir}/RunParameters.xml')
            # if run_parameters_old.is_file():
            #     run_parameters_old.rename(run_parameters)
            #
            # run_parameters = xml.dom.minidom.parse(f'{run_dir}/RunParameters.xml')
            # experiment_name = run_parameters.getElementsByTagName('ExperimentName')[0].firstChild.nodeValue
            # #print (experiment_name)
            # if '_' in experiment_name: #Novaseq exception
            #     experiment_name = experiment_name.split("_")[3]
            # experiment_name = experiment_name.replace('REDO','')
            #
            # project = Project(lims, id=experiment_name)
            # project_name = project.name
            # samples = lims.get_samples(projectlimsid=project.id)
            # # umi = samples[0].udf['UMI']
            # umi = None
            #
            # if rta_complete.is_file() and not conversion_done.is_file() and not conversion_running.is_file() and not conversion_failed.is_file():
            #     #Convert run to FastQ
            #     print (f'Running conversion on {run_dir}')
            #     flowcell = run_dir.name.split("_")[-1]
            #     conversion_log = Path(f'{run_dir}/conversion_log.txt')
            #     conversion_error = Path(f'{run_dir}/conversion_error.txt')
            #
            #     # Copy runParameters.xml to RunParameters.xml, needed for hiseq/miseq conversion
            #     if Path(f'{run_dir}/runParameters.xml').is_file():
            #         os.system(f'cp {run_dir}/runParameters.xml {run_dir}/RunParameters.xml')
            #
            #
            #     if run_parameters.getElementsByTagName('ReagentKitSerial'):  # NextSeq
            #         lims_container_name = run_parameters.getElementsByTagName('ReagentKitSerial')[0].firstChild.nodeValue
            #     elif run_parameters.getElementsByTagName('LibraryTubeSerialBarcode'):  # NovaSeq
            #         lims_container_name = run_parameters.getElementsByTagName('LibraryTubeSerialBarcode')[0].firstChild.nodeValue
            #     elif [x for x in run_dir.glob("*csv")][0] and not sample_sheet.is_file():
            #         v2_samplesheet = [x for x in run_dir.glob("*csv")][0]
            #         v2_samplesheet.rename(f'{run_dir}/SampleSheet.csv')
            #         #v2ToV1SampleSheet(v2_samplesheet,experiment_name, project_name)
            #
            #     if not sample_sheet.is_file():
            #         getSampleSheet(lims, lims_container_name, sample_sheet)
            #
            #     # project_name = getProjectName(sample_sheet)
            #     if sample_sheet.is_file():
            #         # Lock run folder for conversion
            #         conversion_running.touch()
            #
            #         # Convert bcl
            #         exit_code = 1
            #         if umi:
            #             # bases_mask = ''
            #             exit_code = convertBCL(run_dir, conversion_log, conversion_error)
            #         else:
            #             exit_code = convertBCL(run_dir, conversion_log, conversion_error)
            #         # exit_code = 0
            #         if exit_code == 0:  # Conversion completed
            #
            #             # Add flowcell_id to fastq.gz files
            #             addFlowcellToFastq(run_dir, flowcell)
            #
            #             # Generate md5sums for fastq.gz files
            #             md5sumFastq(run_dir)
            #
            #             # Zip conversion report
            #             zip_file = zipConversionReport(run_dir)
            #
            #             # Generate run stats + plots
            #             if sum(generateRunStats(run_dir)) > 0:
            #                 conversion_error = Path(f'{run_dir}/conversion_error.txt')
            #                 ce = conversion_error.open('a')
            #                 ce.write('Conversion probably failed, failed to create conversion statistics. If this is ok please replace the ConversionFailed.txt file with ConversionDone.txt to continue data transfer\n')
            #                 ce.close()
            #                 os.system(f'date >> {conversion_log}')
            #                 conversion_failed.touch()
            #                 conversion_running.unlink()
            #                 conversionFailedMail(run_dir, experiment_name, project_name)
            #
            #
            #             conversion_stats_file = Path(f'{run_dir}/Data/Intensities/BaseCalls/Stats/ConversionStats.xml')
            #             conversion_stats = parseConversionStats( conversion_stats_file  )
            #             total_reads = float(conversion_stats['total_reads'])
            #             undetermined_reads = float(conversion_stats['samples']['Undetermined']['cluster_count'].replace(',',''))
            #             if (undetermined_reads / total_reads) > 0.75:
            #                 #Conversion might have failed due to barcode being in the wrong orientation or due to the presence of UMIs
            #
            #                 #Get barcodes from samplesheet and check if revcomp exists in unknown barcode_mismatches
            #                 rev = None
            #                 clean_bc = None
            #                 dual_index = None
            #                 if not sample_sheet_old.is_file():
            #                     sample_sheet_info = parseSampleSheet(sample_sheet)
            #                     samples = sample_sheet_info['samples']
            #                     header = sample_sheet_info['header']
            #                     for sample in samples:
            #                         if 'N' in sample[header.index('index')] and re.search("[ACGT]", sample[header.index('index')] ) and not umi:
            #                             sample[header.index('index')] = sample[header.index('index')].replace("N","")
            #                             clean_bc = 1
            #                             if 'index2' in header : dual_index = 1
            #                         elif 'index2' in header:
            #                             dual_index = 1
            #                             if 'N' in sample[header.index('index2')]: continue
            #                             revseq = revcomp(sample[header.index('index2')])
            #                             sample[header.index('index2')] = revseq
            #                             index1 = sample[header.index('index')]
            #                             if f'{index1}+{revseq}' in conversion_stats['unknown']: rev = 1
            #                         else:
            #                             revseq = revcomp(sample[header.index('index')])
            #                             sample[header.index('index')] = revseq
            #                             if revseq in conversion_stats['unknown']: rev = 1
            #
            #
            #                     conversion_error = Path(f'{run_dir}/conversion_error.txt')
            #                     ce = conversion_error.open('a')
            #
            #                     if rev or clean_bc:
            #                         sample_sheet.rename(sample_sheet_old)
            #                         writeSampleSheet(run_dir,header, samples, sample_sheet_info['top'])
            #                         ce.write('Conversion probably failed, >75% of reads in Undetermined fraction. Trying to remove N\'s and/or reverse complementation.\n')
            #                         conversion_running.unlink()
            #                     else:
            #                         ce.write('Conversion probably failed, >75% of reads in Undetermined fraction. No solution could be found automatically.\n')
            #                         conversion_failed.touch()
            #                         conversion_running.unlink()
            #                         conversionFailedMail(run_dir, experiment_name, project_name)
            #                     ce.close()
            #
            #             else:
            #                 # Remove lock and set conversion done
            #                 os.system(f'date >> {conversion_log}')
            #                 conversion_done.touch()
            #                 conversion_running.unlink()
            #         #
            #         else:  # Conversion failed
            #             # Remove lock and set conversion done
            #             os.system(f'date >> {conversion_log}')
            #             conversion_failed.touch()
            #             conversion_running.unlink()
            #         #
            #             # Send failed conversion email
            #             conversionFailedMail(run_dir, experiment_name, project_name)
            #
            #
            #
            # # Transfer run
            # if conversion_done.is_file() and not transfer_running.is_file() and not transfer_done.is_file() and not transfer_failed.is_file():
            #     print (f'Running transfer on {run_dir}')
            #     machine = run_dir.parents[0].name
            #     # project_name = getProjectName(sample_sheet)
            #     zip_log = f'{run_dir}/run_zip.log'
            #     zip_error = f'{run_dir}/run_zip.err'
            #     zipped_run = Path(f'{STAGING_DIR}/{experiment_name}-raw.tar')
            #     zip_done = Path(f'{STAGING_DIR}/{experiment_name}-raw.tar.done')
            #     zip_command = f'cd {run_dir.parents[0]} && tar -cf {zipped_run} --exclude "*jpg" --exclude "*bcl*" --exclude "*.filter" --exclude "*tif" --exclude "*run_zip.*" {run_dir.name} 1>> {zip_log} 2>> {zip_error}'
            #
            #     transfer_log = f'{run_dir}/transfer.log'
            #     transfer_error = f'{run_dir}/transfer.err'
            #     transfer_command = f'scp {zipped_run} {NEXTCLOUD_HOST}:{NEXTCLOUD_DATA_ROOT}/{NEXTCLOUD_RAW_DIR} 1>> {transfer_log} 2>> {transfer_error}'
            #
            #
            #     analysis_steps = samples[0].udf['Analysis'].split(',')
            #     print(analysis_steps)
            #
            #     rsync_command = '/usr/bin/rsync -rah --update --stats --verbose --prune-empty-dirs'
            #     if len(analysis_steps) > 1 or umi:
            #         rsync_command += " --include '*.fq.gz' --include '*.fastq.gz'"
            #     rsync_command += " --include '*/' --include 'md5sum.txt' --include 'SampleSheet.csv' --include 'RunInfo.xml' --include '*unParameters.xml' --include 'InterOp/**' --include '*/*/Reports/**' --include 'Data/Intensities/BaseCalls/Stats/**' --include '*.[pP][eE][dD]'"
            #     rsync_command += " --exclude '*'"
            #     rsync_command += f" {run_dir}"
            #     rsync_command += f" {DATA_DIR_HPC}/{machine} 1>> {transfer_log} 2>> {transfer_error}"
            #     print(f'{transfer_command} && {rsync_command}')
            #     transfer_command = f'{transfer_command} && {rsync_command}'
            #
            #     transfer_running.touch()
            #
            #     try:
            #         exit_code = 0
            #         if not zip_done.is_file():
            #
            #             exit_code = os.system(zip_command)
            #             # print (exit_code)
            #             if not exit_code: zip_done.touch()
            #
            #         if zip_done.is_file():
            #             remote_run_path = f'{NEXTCLOUD_RAW_DIR}/{run_dir.name}.tar'
            #
            #             exit_code = os.system(transfer_command)
            #
            #             # project_name = getProjectName(sample_sheet)
            #             if not exit_code:
            #                 transfer_done.touch()
            #                 transfer_running.unlink()
            #                 zipped_run.unlink()
            #                 zip_done.unlink()
            #                 conversionSuccesMail(run_dir, experiment_name, project_name)
            #
            #
            #             else:
            #                 transfer_failed.touch()
            #                 transfer_running.unlink()
            #                 transferFailedMail(run_dir, experiment_name, project_name)
            #     except:
            #         logger.exception('Unhandled Exception')
            #
            # if transfer_done.is_file() and not archive_running.is_file() and not archive_done.is_file() and not archive_failed.is_file():
            #     print (f'Running archive on {run_dir}')
            #     machine = run_dir.parents[0].name
            #     archive_log = Path(f'{run_dir}/illumina_archive.log')
            #     archive_error = Path(f'{run_dir}/illumina_archive.err')
            #     # project_name = getProjectName(sample_sheet)
            #     try:
            #         archive_running.touch()
            #         rsync_command = f"rsync -rahm --exclude '*jpg' --exclude '*fastq.gz' --exclude '*fq.gz' {run_dir} {ARCHIVE_DIR}/{machine} 1>> {archive_log} 2>> {archive_error}"
            #         exit_code = os.system(rsync_command)
            #
            #         if not exit_code:
            #             archive_done.touch()
            #             archive_running.unlink()
            #
            #             for file in run_dir.glob("**/*.gz"):
            #                 # print (file.name)
            #                 if file.name.endswith(".fastq.gz") or file.name.endswith(".fq.gz"):
            #                     # print (file.name)
            #                     file.unlink()
            #         else:
            #             archive_failed.touch()
            #             archive_running.unlink()
            #             archiveFailedMail(run_dir, experiment_name, project_name)
            #     except Exception as e:
            #         logger.exception('Unhandled Exception')



def run(lims, missing_bcl, barcode_mismatches, fastq_for_index, short_reads,bases_mask):
    """Runs the manageRuns function"""
    global nextcloud_util
    #Set up nextcloud
    nextcloud_util = NextcloudUtil()
    nextcloud_util.setHostname( NEXTCLOUD_HOST )
    nextcloud_util.setup( NEXTCLOUD_USER, NEXTCLOUD_PW, NEXTCLOUD_WEBDAV_ROOT,NEXTCLOUD_RAW_DIR,MAIL_SENDER )
    # options = {
    #      'webdav_hostname': f"https://{NEXTCLOUD_HOST}",
    #      'webdav_login':    NEXTCLOUD_USER,
    #      'webdav_password': NEXTCLOUD_PW
    # }
    # client = Client(options)


    manageRuns(lims )
