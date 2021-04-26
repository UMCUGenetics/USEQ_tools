import os
import argparse
import subprocess
import logging
import logging.handlers
import xml.dom.minidom
import re
from config import DATA_DIRS_RAW,DATA_DIR_HPC,ARCHIVE_DIR,MAIL_SENDER,MAIL_ADMINS,INTEROP_PATH,RUNTYPE_YIELDS,BCL2FASTQ_PATH,BCL2FASTQ_PROCESSING_THREADS,BCL2FASTQ_WRITING_THREADS,STAGING_DIR,NEXTCLOUD_DATA_ROOT,NEXTCLOUD_PW,NEXTCLOUD_USER,NEXTCLOUD_HOST,NEXTCLOUD_RAW_DIR
from modules.useq_mail import sendMail
from modules.useq_illumina_parsers import parseConversionStats, parseRunParameters,parseSampleSheet

from pathlib import Path
from modules.useq_template import TEMPLATE_PATH,TEMPLATE_ENVIRONMENT,renderTemplate
from datetime import datetime
from webdav3.client import Client
from genologics.entities import Project

def revcomp(seq):
    revcompl = lambda x: ''.join([{'A':'T','C':'G','G':'C','T':'A'}[B] for B in x][::-1])
    return revcompl(seq)




def convertBCL(run_dir, log_file, error_file, missing_bcl, barcode_mismatches, fastq_for_index, short_reads, bases_mask):
    """Convert bcl files from run to fastq.gz files."""

    # Start conversion
    os.system(f'date >> {log_file}')
    command = f'{BCL2FASTQ_PATH}/bin/bcl2fastq --runfolder-dir {run_dir} -r {BCL2FASTQ_PROCESSING_THREADS} -p {BCL2FASTQ_PROCESSING_THREADS} -w {BCL2FASTQ_WRITING_THREADS} -l WARNING --barcode-mismatches {barcode_mismatches}'

    if missing_bcl: command = f'{command} --ignore-missing-bcls'
    if fastq_for_index: command = f'{command} --create-fastq-for-index-reads'
    if short_reads: command = f'{command} --minimum-trimmed-read-length 0 --mask-short-adapter-reads 0'
    if bases_mask : command = f'{command} --use-bases-mask {bases_mask}'

    command = f'{command} 1>> {log_file} 2>> {error_file}'
    exit_code = os.system(command)
    return exit_code
#
def addFlowcellToFastq(run_dir, flowcell_id):
    """Add flowcell id to fastq.gz filename."""
    base_calls_dir = Path(f'{run_dir}/Data/Intensities/BaseCalls')
    for fastq in base_calls_dir.rglob("*.fastq.gz"):
        filename_parts = fastq.name.split('_')
        if filename_parts[1] != flowcell_id:
            filename_parts.insert(1, flowcell_id)
            new_filename = '_'.join(filename_parts)
            fastq.rename(f'{fastq.parent}/{new_filename}')


#
def zipConversionReport(run_dir):
    """Zip conversion reports."""
    zip_file = f'{run_dir}/{run_dir.name}_Reports.zip'
    os.chdir(f'{run_dir}/Data/Intensities/BaseCalls/')
    os.system(f'zip -FSr {zip_file} Reports/')
    return zip_file
#
def md5sumFastq(run_dir):
    """Generate md5sums for all fastq.gz files from a sequencing run."""
    command = f'(cd {run_dir}/Data/Intensities/BaseCalls/ && find . -type f -iname "*.fastq.gz" -exec md5sum {{}} \\; > md5sum.txt)'
    os.system(command)

def generateRunStats(run_dir):
    """Create run stats files using interop tool."""
    stats_dir = Path(f'{run_dir}/Data/Intensities/BaseCalls/Stats')

    if not stats_dir.is_dir():
        # Create 'Stats' dir
        stats_dir.mkdir()

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
    return exit_codes

def writeV1SampleSheet(dir, header, samples, top):
    # data = {
    #     'top' : top,
    #     'header' : header,
    #     'samples' : samples,
    # }

    with open(f'{dir}/SampleSheet.csv', 'w') as new_sheet:
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

    ce = open( f'{run_dir}/conversion_error.txt', 'r')
    cl = open( f'{run_dir}/conversion_log.txt', 'r')
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
    try:
        conversion_stats = parseConversionStats( f'{run_dir}/Data/Intensities/BaseCalls/Stats/ConversionStats.xml' )
    except:
        error = f'Failed to find ConversionStats.xml for {experiment_name}'

    if not error:
        attachments = {
            'zip_file': f'{run_dir}/{run_dir.name}_Reports.zip',
            'basepercent_by_cycle_plot': f'{run_dir}/Data/Intensities/BaseCalls/Stats/{run_dir.name}_BasePercent-by-cycle_BasePercent.png',
            'intensity_by_cycle_plot': f'{run_dir}/Data/Intensities/BaseCalls/Stats/{run_dir.name}_Intensity-by-cycle_Intensity.png',
            'clusterdensity_by_lane_plot': f'{run_dir}/Data/Intensities/BaseCalls/Stats/{run_dir.name}_Clusters-by-lane.png',
            'flowcell_intensity_plot': f'{run_dir}/Data/Intensities/BaseCalls/Stats/{run_dir.name}_flowcell-Intensity.png',
            'q_heatmap_plot': f'{run_dir}/Data/Intensities/BaseCalls/Stats/{run_dir.name}_q-heat-map.png',
            'q_histogram_plot': f'{run_dir}/Data/Intensities/BaseCalls/Stats/{run_dir.name}_q-histogram.png',
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

    transfer_log = Path(f'{run_dir}/transfer.log')
    transfer_error = Path(f'{run_dir}/transfer.err')
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
    archive_error = Path(f'{run_dir}/illumina_archive.err')
    archive_log = Path(f'{run_dir}/illumina_archive.log')
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


def manageRuns(lims, missing_bcl, barcode_mismatches, fastq_for_index, short_reads, bases_mask):
    transfers = {}  # Used to keep track of started transfer processes
    archive_transfers = {} # Used to keep track of started archive transfer processes
    smtp_handler = logging.handlers.SMTPHandler(
        mailhost=("smtp-open.umcutrecht.nl", 25),
        fromaddr=MAIL_SENDER,
        toaddrs=MAIL_ADMINS,
        subject=u"Error occured during conversion!")

    logger = logging.getLogger()
    logger.addHandler(smtp_handler)

    for machine_dir in DATA_DIRS_RAW:
        #print(machine_dir)
        md_path = Path(machine_dir)
        for run_dir in md_path.glob("*"):
            #print ('Checking: ',run_dir)
            if run_dir.name.count('_') != 3 or not run_dir.is_dir(): continue
            #Important Files
            sample_sheet = Path(f'{run_dir}/SampleSheet.csv')
            sample_sheet_old = Path(f'{run_dir}/SampleSheet.csv-old')
            rta_complete = Path(f'{run_dir}/RTAComplete.txt')
            conversion_done = Path(f'{run_dir}/ConversionDone.txt')
            conversion_running = Path(f'{run_dir}/ConversionRunning.txt')
            conversion_failed = Path(f'{run_dir}/ConversionFailed.txt')
            transfer_running = Path(f'{run_dir}/TransferRunning.txt')
            transfer_done = Path(f'{run_dir}/TransferDone.txt')
            transfer_failed = Path(f'{run_dir}/TransferFailed.txt')
            archive_done = Path(f'{run_dir}/ArchiveDone.txt')
            archive_running = Path(f'{run_dir}/ArchiveRunning.txt')
            archive_failed = Path(f'{run_dir}/ArchiveFailed.txt')
            run_parameters_old = Path(f'{run_dir}/runParameters.xml')
            run_parameters = Path(f'{run_dir}/RunParameters.xml')
            if run_parameters_old.is_file():
                run_parameters_old.rename(run_parameters)


            run_parameters = xml.dom.minidom.parse(f'{run_dir}/RunParameters.xml')
            experiment_name = run_parameters.getElementsByTagName('ExperimentName')[0].firstChild.nodeValue
            #print (experiment_name)
            if '_' in experiment_name: #Novaseq exception
                experiment_name = experiment_name.split("_")[3]
            experiment_name = experiment_name.replace('REDO','')
            print(run_dir, experiment_name)
            project = Project(lims, id=experiment_name)
            project_name = project.name
            samples = lims.get_samples(projectlimsid=project.id)
            # umi = samples[0].udf['UMI']
            umi = None

            if rta_complete.is_file() and not conversion_done.is_file() and not conversion_running.is_file() and not conversion_failed.is_file():
                #Convert run to FastQ
                print (f'Running conversion on {run_dir}')
                flowcell = run_dir.name.split("_")[-1]
                conversion_log = Path(f'{run_dir}/conversion_log.txt')
                conversion_error = Path(f'{run_dir}/conversion_error.txt')

                # Copy runParameters.xml to RunParameters.xml, needed for hiseq/miseq conversion
                if Path(f'{run_dir}/runParameters.xml').is_file():
                    os.system(f'cp {run_dir}/runParameters.xml {run_dir}/RunParameters.xml')


                if run_parameters.getElementsByTagName('ReagentKitSerial'):  # NextSeq
                    lims_container_name = run_parameters.getElementsByTagName('ReagentKitSerial')[0].firstChild.nodeValue
                elif run_parameters.getElementsByTagName('LibraryTubeSerialBarcode'):  # NovaSeq
                    lims_container_name = run_parameters.getElementsByTagName('LibraryTubeSerialBarcode')[0].firstChild.nodeValue
                elif [x for x in run_dir.glob("*csv")][0] and not sample_sheet.is_file():
                    v2_samplesheet = [x for x in run_dir.glob("*csv")][0]
                    v2_samplesheet.rename(f'{run_dir}/SampleSheet.csv')
                    #v2ToV1SampleSheet(v2_samplesheet,experiment_name, project_name)

                if not sample_sheet.is_file():
                    getSampleSheet(lims, lims_container_name, sample_sheet)

                # project_name = getProjectName(sample_sheet)
                if sample_sheet.is_file():
                    # Lock run folder for conversion
                    conversion_running.touch()

                    # Convert bcl
                    exit_code = 1
                    if umi:
                        # bases_mask = ''
                        exit_code = convertBCL(run_dir, conversion_log, conversion_error, missing_bcl, barcode_mismatches, fastq_for_index, short_reads, bases_mask)
                    else:
                        exit_code = convertBCL(run_dir, conversion_log, conversion_error, missing_bcl, barcode_mismatches, fastq_for_index, short_reads, bases_mask)
                    # exit_code = 0
                    if exit_code == 0:  # Conversion completed

                        # Add flowcell_id to fastq.gz files
                        addFlowcellToFastq(run_dir, flowcell)

                        # Generate md5sums for fastq.gz files
                        md5sumFastq(run_dir)

                        # Zip conversion report
                        zip_file = zipConversionReport(run_dir)

                        # Generate run stats + plots
                        if sum(generateRunStats(run_dir)) > 0:
                            conversion_error = Path(f'{run_dir}/conversion_error.txt')
                            ce = conversion_error.open('a')
                            ce.write('Conversion probably failed, failed to create conversion statistics. If this is ok please replace the ConversionFailed.txt file with ConversionDone.txt to continue data transfer\n')
                            ce.close()
                            os.system(f'date >> {conversion_log}')
                            conversion_failed.touch()
                            conversion_running.unlink()
                            conversionFailedMail(run_dir, experiment_name, project_name)


                        conversion_stats_file = Path(f'{run_dir}/Data/Intensities/BaseCalls/Stats/ConversionStats.xml')
                        conversion_stats = parseConversionStats( conversion_stats_file  )
                        total_reads = float(conversion_stats['total_reads'])
                        undetermined_reads = float(conversion_stats['samples']['Undetermined']['cluster_count'].replace(',',''))
                        if (undetermined_reads / total_reads) > 0.75:
                            #Conversion might have failed due to barcode being in the wrong orientation or due to the presence of UMIs

                            #Get barcodes from samplesheet and check if revcomp exists in unknown barcode_mismatches
                            rev = None
                            clean_bc = None
                            dual_index = None
                            if not sample_sheet_old.is_file():
                                sample_sheet_info = parseSampleSheet(sample_sheet)
                                samples = sample_sheet_info['samples']
                                header = sample_sheet_info['header']
                                for sample in samples:
                                    if 'N' in sample[header.index('index')] and re.search("[ACGT]", sample[header.index('index')] ) and not umi:
                                        sample[header.index('index')] = sample[header.index('index')].replace("N","")
                                        clean_bc = 1
                                        if 'index2' in header : dual_index = 1
                                    elif 'index2' in header:
                                        dual_index = 1
                                        if 'N' in sample[header.index('index2')]: continue
                                        revseq = revcomp(sample[header.index('index2')])
                                        sample[header.index('index2')] = revseq
                                        index1 = sample[header.index('index')]
                                        if f'{index1}+{revseq}' in conversion_stats['unknown']: rev = 1
                                    else:
                                        revseq = revcomp(sample[header.index('index')])
                                        sample[header.index('index')] = revseq
                                        if revseq in conversion_stats['unknown']: rev = 1


                                conversion_error = Path(f'{run_dir}/conversion_error.txt')
                                ce = conversion_error.open('a')

                                if rev or clean_bc:
                                    sample_sheet.rename(sample_sheet_old)
                                    writeV1SampleSheet(run_dir,header, samples, sample_sheet_info['top'])
                                    ce.write('Conversion probably failed, >75% of reads in Undetermined fraction. Trying to remove N\'s and/or reverse complementation.\n')
                                    conversion_running.unlink()
                                else:
                                    ce.write('Conversion probably failed, >75% of reads in Undetermined fraction. No solution could be found automatically.\n')
                                    conversion_failed.touch()
                                    conversion_running.unlink()
                                    conversionFailedMail(run_dir, experiment_name, project_name)
                                ce.close()

                        else:
                            # Remove lock and set conversion done
                            os.system(f'date >> {conversion_log}')
                            conversion_done.touch()
                            conversion_running.unlink()
                    #
                    else:  # Conversion failed
                        # Remove lock and set conversion done
                        os.system(f'date >> {conversion_log}')
                        conversion_failed.touch()
                        conversion_running.unlink()
                    #
                        # Send failed conversion email
                        conversionFailedMail(run_dir, experiment_name, project_name)



            # Transfer run
            if conversion_done.is_file() and not transfer_running.is_file() and not transfer_done.is_file() and not transfer_failed.is_file():
                print (f'Running transfer on {run_dir}')
                machine = run_dir.parents[0].name
                # project_name = getProjectName(sample_sheet)
                zip_log = f'{run_dir}/run_zip.log'
                zip_error = f'{run_dir}/run_zip.err'
                zipped_run = Path(f'{STAGING_DIR}/{experiment_name}-raw.tar')
                zip_done = Path(f'{STAGING_DIR}/{experiment_name}-raw.tar.done')
                zip_command = f'cd {run_dir.parents[0]} && tar -cf {zipped_run} --exclude "*jpg" --exclude "*bcl*" --exclude "*.filter" --exclude "*tif" --exclude "*run_zip.*" {run_dir.name} 1>> {zip_log} 2>> {zip_error}'

                transfer_log = f'{run_dir}/transfer.log'
                transfer_error = f'{run_dir}/transfer.err'
                transfer_command = f'scp {zipped_run} {NEXTCLOUD_HOST}:{NEXTCLOUD_DATA_ROOT}/{NEXTCLOUD_RAW_DIR} 1>> {transfer_log} 2>> {transfer_error}'


                analysis_steps = samples[0].udf['Analysis'].split(',')
                print(analysis_steps)

                rsync_command = '/usr/bin/rsync -rah --update --stats --verbose --prune-empty-dirs'
                if len(analysis_steps) > 1 or umi:
                    rsync_command += " --include '*.fq.gz' --include '*.fastq.gz'"
                rsync_command += " --include '*/' --include 'md5sum.txt' --include 'SampleSheet.csv' --include 'RunInfo.xml' --include '*unParameters.xml' --include 'InterOp/**' --include '*/*/Reports/**' --include 'Data/Intensities/BaseCalls/Stats/*' --include '*.[pP][eE][dD]'"
                rsync_command += " --exclude '*'"
                rsync_command += f" {run_dir}"
                rsync_command += f" {DATA_DIR_HPC}/{machine} 1>> {transfer_log} 2>> {transfer_error}"
                print(f'{transfer_command} && {rsync_command}')
                transfer_command = f'{transfer_command} && {rsync_command}'

                transfer_running.touch()

                try:
                    exit_code = 0
                    if not zip_done.is_file():

                        exit_code = os.system(zip_command)
                        # print (exit_code)
                        if not exit_code: zip_done.touch()

                    if zip_done.is_file():
                        remote_run_path = f'{NEXTCLOUD_RAW_DIR}/{run_dir.name}.tar'

                        exit_code = os.system(transfer_command)

                        # project_name = getProjectName(sample_sheet)
                        if not exit_code:
                            transfer_done.touch()
                            transfer_running.unlink()
                            zipped_run.unlink()
                            zip_done.unlink()
                            conversionSuccesMail(run_dir, experiment_name, project_name)


                        else:
                            transfer_failed.touch()
                            transfer_running.unlink()
                            transferFailedMail(run_dir, experiment_name, project_name)
                except:
                    logger.exception('Unhandled Exception')

            if transfer_done.is_file() and not archive_running.is_file() and not archive_done.is_file() and not archive_failed.is_file():
                print (f'Running archive on {run_dir}')
                machine = run_dir.parents[0].name
                archive_log = Path(f'{run_dir}/illumina_archive.log')
                archive_error = Path(f'{run_dir}/illumina_archive.err')
                # project_name = getProjectName(sample_sheet)
                try:
                    archive_running.touch()
                    rsync_command = f"rsync -rahm --exclude '*jpg' --exclude '*fastq.gz' --exclude '*fq.gz' {run_dir} {ARCHIVE_DIR}/{machine} 1>> {archive_log} 2>> {archive_error}"
                    exit_code = os.system(rsync_command)

                    if not exit_code:
                        archive_done.touch()
                        archive_running.unlink()

                        for file in run_dir.glob("**/*.gz"):
                            # print (file.name)
                            if file.name.endswith(".fastq.gz") or file.name.endswith(".fq.gz"):
                                # print (file.name)
                                file.unlink()
                    else:
                        archive_failed.touch()
                        archive_running.unlink()
                        archiveFailedMail(run_dir, experiment_name, project_name)
                except Exception as e:
                    logger.exception('Unhandled Exception')



def run(lims, missing_bcl, barcode_mismatches, fastq_for_index, short_reads,bases_mask):
    """Runs the manageRuns function"""

    # options = {
    #      'webdav_hostname': f"https://{NEXTCLOUD_HOST}",
    #      'webdav_login':    NEXTCLOUD_USER,
    #      'webdav_password': NEXTCLOUD_PW
    # }
    # client = Client(options)


    manageRuns(lims, missing_bcl, barcode_mismatches, fastq_for_index, short_reads,bases_mask )
