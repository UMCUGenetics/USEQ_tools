import os
import argparse
import subprocess
import logging
import logging.handlers
import xml.dom.minidom
from config import DATA_DIR_CONVERSION,DATA_DIR_HPC,ARCHIVE_DIR,MAIL_SENDER,MAIL_ADMINS,INTEROP_PATH,RUNTYPE_YIELDS,BCL2FASTQ_PATH,BCL2FASTQ_PROCESSING_THREADS,BCL2FASTQ_WRITING_THREADS
from modules.useq_template import TEMPLATE_PATH,TEMPLATE_ENVIRONMENT,renderTemplate
from modules.useq_mail import sendMail
from modules.useq_illumina_parsers import parseConversionStats, parseRunParameters

def walkLevel(some_dir, level=1):
    some_dir = some_dir.rstrip(os.path.sep)
    assert os.path.isdir(some_dir)
    num_sep = some_dir.count(os.path.sep)
    for root, dirs, files in os.walk(some_dir):
        yield root, dirs, files
        num_sep_this = root.count(os.path.sep)
        if num_sep + level <= num_sep_this:
            del dirs[:]

# args.missing_bcl, args.barcode_mismatches, args.fastq_for_index, args.short_reads
def convertBCL(run_dir, log_file, error_file, missing_bcl, barcode_mismatches, fastq_for_index, short_reads):
    """Convert bcl files from run to fastq.gz files."""

    # Start conversion
    os.system('date >> {}'.format(log_file))
    command = '{bcl2fastq}/bin/bcl2fastq --runfolder-dir {dir} -r {processing_threads} -p {processing_threads} -w {writing_threads} -l {log_level} --barcode-mismatches {barcode_mismatches} '.format(
        bcl2fastq=BCL2FASTQ_PATH,
        dir=run_dir,
        processing_threads=BCL2FASTQ_PROCESSING_THREADS,
        writing_threads=BCL2FASTQ_WRITING_THREADS,
        log_level='WARNING',
        barcode_mismatches=barcode_mismatches
    )

    if missing_bcl:
        command = '{} {} '.format(
            command,
            "--ignore-missing-bcls "
        )
    if fastq_for_index:
        command = '{} {} '.format(
            command,
            "--create-fastq-for-index-reads "
        )

    if short_reads:
        command = '{} {} '.format(
            command,
            "--minimum-trimmed-read-length 0 --mask-short-adapter-reads 0 "
        )

    command = '{} 1>> {log_file} 2>> {error_file}'.format(
        command,
        log_file=log_file,
        error_file=error_file
    )
    exit_code = os.system(command)
    return exit_code

def addFlowcellToFastq(run_dir, flowcell_id):
    """Add flowcell id to fastq.gz filename."""
    base_calls_dir = '{}/{}'.format(run_dir, 'Data/Intensities/BaseCalls')
    for root, dirs, files in os.walk(base_calls_dir):
        for filename in files:
            if filename.endswith('.fastq.gz'):
                filename_parts = filename.split('_')
                if filename_parts[1] != flowcell_id:
                    filename_parts.insert(1, flowcell_id)
                    new_filename = '_'.join(filename_parts)
                    os.rename(os.path.join(root, filename), os.path.join(root, new_filename))

def zipConversionReport(run_dir, run_name):
    """Zip conversion reports."""
    zip_file = '{}/{}_Reports.zip'.format(run_dir, run_name)
    os.chdir('{}/Data/Intensities/BaseCalls/'.format(run_dir))
    os.system('zip -FSr {} Reports/'.format(zip_file))
    return zip_file

def md5sumFastq(run_dir):
    """Generate md5sums for all fastq.gz files from a sequencing run."""
    command = '(cd {run_dir}/{base_calls_dir} && find . -type f -iname "*.fastq.gz" -exec md5sum {{}} \\; > md5sum.txt)'.format(
        run_dir=run_dir,
        base_calls_dir='Data/Intensities/BaseCalls/'
    )
    os.system(command)

def generateRunStats(run_dir, run_name):
    """Create run stats files using interop tool."""
    stats_dir = "{0}/Data/Intensities/BaseCalls/Stats".format(run_dir)

    if not os.path.isdir(stats_dir):
        # Create 'Stats' dir
        os.mkdir(stats_dir)

    exit_code = 0
    os.chdir(stats_dir)
    # Run summary csv
    exit_code = os.system("{0}/bin/summary {1} > {2}/{3}_summary.csv".format(INTEROP_PATH, run_dir, stats_dir, run_name))
    # Index summary csv
    exit_code = os.system("{0}/bin/index-summary {1} --csv= > {2}/{3}_index-summary.csv".format(INTEROP_PATH, run_dir, stats_dir, run_name))
    # Intensity by cycle plot
    exit_code = os.system("{0}/bin/plot_by_cycle {1} --metric-name=Intensity | gnuplot".format(INTEROP_PATH, run_dir))
    # % Base by cycle plot
    exit_code = os.system("{0}/bin/plot_by_cycle {1} --metric-name=BasePercent | gnuplot".format(INTEROP_PATH, run_dir))
    # Clustercount by lane plot
    exit_code = os.system("{0}/bin/plot_by_lane {1} --metric-name=Clusters | gnuplot".format(INTEROP_PATH, run_dir))
    # Flowcell intensity plot
    exit_code = os.system("{0}/bin/plot_flowcell {1} | gnuplot".format(INTEROP_PATH, run_dir))
    # QScore heatmap plot
    exit_code = os.system("{0}/bin/plot_qscore_heatmap {1} | gnuplot".format(INTEROP_PATH, run_dir))
    # QScore histogram plot
    exit_code = os.system("{0}/bin/plot_qscore_histogram {1} | gnuplot".format(INTEROP_PATH, run_dir))
    return exit_code


def getSampleSheet(lims, container_name, sample_sheet_path):
    """Get sample_sheet from clarity lims and write to sample_sheet_path."""
    for reagent_kit_artifact in lims.get_artifacts(containername=container_name):
        process = reagent_kit_artifact.parent_process
        for artifact in process.result_files():
            if artifact.name == 'Sample Sheet' and artifact.files:
                file_id = artifact.files[0].id
                sample_sheet = lims.get_file_contents(id=file_id)
                sample_sheet_file = open(sample_sheet_path, 'w')
                sample_sheet_file.write(sample_sheet)
                return True
    return False

def getProjectName(sample_sheet):
    project_name = None
    with open(sample_sheet, 'r') as f:

        for line in f:
            if line.startswith('Project Name'):
                project_name = line.rstrip().split(",")[-1]

    return project_name

def conversionFailedMail(run_dir, experiment_name, project_name):
    ce = open(os.path.join(run_dir, 'conversionError.txt'), 'r')
    cl = open(os.path.join(run_dir, 'conversionLog.txt'), 'r')
    template_data = {
        'experiment_name' : experiment_name,
        'run_dir' : run_dir,
        'conversion_error' : ce.read(),
        'conversion_log' : cl.read()
    }
    mail_content = renderTemplate('conversion_failed_template.html', template_data)
    mail_subject = 'ERROR: Conversion of {0} - {1} failed!'.format(experiment_name, project_name)
    sendMail(mail_subject,mail_content, MAIL_SENDER ,MAIL_ADMINS)
    ce.close()
    cl.close()

def conversionSuccesMail(run_dir, experiment_name, project_name):
    run_name = os.path.split(run_dir)[-1]
    machine = os.path.split(run_dir)[-2]
    subject = 'Conversion of {0} - {1} finished'.format(experiment_name, project_name)
    attachments = None
    template_data = None
    content = None
    expected_reads = parseRunParameters( "{0}/RunParameters.xml".format(run_dir) )
    conversion_stats = parseConversionStats( "{0}/Data/Intensities/BaseCalls/Stats/ConversionStats.xml".format(run_dir) )
    # print conversion_stats, "\n\n"
    # print conversion_stats['unknown']
    attachments = {
        'zip_file': '{}/{}_Reports.zip'.format(run_dir, run_name),
        'basepercent_by_cycle_plot': '{}/Data/Intensities/BaseCalls/Stats/{}_BasePercent-by-cycle_BasePercent.png'.format(run_dir, run_name),
        'intensity_by_cycle_plot': '{}/Data/Intensities/BaseCalls/Stats/{}_Intensity-by-cycle_Intensity.png'.format(run_dir, run_name),
        'clusterdensity_by_lane_plot': '{}/Data/Intensities/BaseCalls/Stats/{}_Clusters-by-lane.png'.format(run_dir, run_name),
        'flowcell_intensity_plot': '{}/Data/Intensities/BaseCalls/Stats/{}_flowcell-Intensity.png'.format(run_dir, run_name),
        'q_heatmap_plot': '{}/Data/Intensities/BaseCalls/Stats/{}_q-heat-map.png'.format(run_dir, run_name),
        'q_histogram_plot': '{}/Data/Intensities/BaseCalls/Stats/{}_q-histogram.png'.format(run_dir, run_name)
    }
    template_data = {
        'experiment_name': experiment_name,
        'run_dir': run_name,
        'rsync_location': " {0}".format( machine),
        'nr_reads': "{0:,} / {1:,} / {2:,}".format(conversion_stats['total_reads'], conversion_stats['total_reads_raw'], expected_reads),
        'stats_summary': conversion_stats
    }
    mail_content = renderTemplate('conversion_done_template.html', template_data)
    # print content
    mail_subject = 'Conversion of {0} - {1} finished'.format(experiment_name, project_name)
    sendMail(mail_subject,mail_content, MAIL_SENDER ,MAIL_ADMINS, attachments=attachments)

def transferFailedMail(run_dir, experiment_name, project_name):
    te = open(os.path.join(run_dir, 'illumina_transfer.err'), 'r')
    tl = open(os.path.join(run_dir, 'illumina_transfer.log'), 'r')
    template_data = {
        'experiment_name' : experiment_name,
        'run_dir' : run_dir,
        'transfer_error' : te.read(),
        'transfer_log' : tl.read()
    }
    mail_content = renderTemplate('transfer_failed_template.html', template_data)
    mail_subject = 'ERROR: Transfer of {0} - {1} failed!'.format(experiment_name, project_name)
    sendMail(mail_subject,mail_content, MAIL_SENDER ,MAIL_ADMINS)

def archiveFailedMail(run_dir, experiment_name, project_name):
    ae = open(os.path.join(run_dir, 'illumina_archive.err'), 'r')
    al = open(os.path.join(run_dir, 'illumina_archive.log'), 'r')
    template_data = {
        'experiment_name' : experiment_name,
        'run_dir' : run_dir,
        'archive_error' : ae.read(),
        'archive_log' : al.read()
    }
    mail_content = renderTemplate('transfer_failed_template.html', template_data)
    mail_subject = 'ERROR: Transfer of {0} - {1} failed!'.format(experiment_name, project_name)
    sendMail(mail_subject,mail_content, MAIL_SENDER ,MAIL_ADMINS)


def manageRuns(lims, missing_bcl, barcode_mismatches, fastq_for_index, short_reads):
    transfers = {}  # Used to keep track of started transfer processes
    archive_transfers = {} # Used to keep track of started archive transfer processes
    smtp_handler = logging.handlers.SMTPHandler(
        mailhost=("smtp-open.umcutrecht.nl", 25),
        fromaddr=MAIL_SENDER,
        toaddrs=MAIL_ADMINS,
        subject=u"Error occured during conversion!")

    logger = logging.getLogger()
    logger.addHandler(smtp_handler)

    try:
        for root,dirs,files in walkLevel(DATA_DIR_CONVERSION, level=1):
            for run_dir in dirs:
                run_dir = os.path.join(root,run_dir)
                #Important files
                sample_sheet = os.path.join(run_dir,'SampleSheet.csv')
                rta_complete = os.path.join(run_dir,'RTAComplete.txt')
                conversion_done = os.path.join(run_dir, 'ConversionDone.txt')
                conversion_running = os.path.join(run_dir, 'ConversionRunning.txt')
                conversion_failed = os.path.join(run_dir, 'ConversionFailed.txt')
                transfer_running = os.path.join(run_dir, 'TransferRunning.txt')
                transfer_done = os.path.join(run_dir, 'TransferDone.txt')
                transfer_failed = os.path.join(run_dir,'TransferFailed.txt')
                archive_done = os.path.join(run_dir,'ArchiveDone.txt')
                archive_running = os.path.join(run_dir,'ArchiveRunning.txt')
                archive_failed = os.path.join(run_dir, 'ArchiveFailed.txt')
                #Convert run to FastQ
                if os.path.isfile(rta_complete) and not os.path.isfile(conversion_done) and not os.path.isfile(conversion_running) and not os.path.isfile(conversion_failed):
                    # Setup conversion metadata
                    run_name = os.path.split(run_dir)[-1]
                    flowcell_id = run_name.split("_")[-1]
                    conversion_log = os.path.join(run_dir, 'conversionLog.txt')
                    conversion_error = os.path.join(run_dir, 'conversionError.txt')


                    # Copy runParameters.xml to RunParameters.xml, needed for hiseq/miseq conversion.
                    if os.path.isfile(os.path.join(run_dir, 'runParameters.xml')):
                        os.system('cp {dir}/{source} {dir}/{dest}'.format(
                            dir=run_dir,
                            source='runParameters.xml',
                            dest='RunParameters.xml'
                        ))

                    run_parameters = xml.dom.minidom.parse(os.path.join(run_dir,'RunParameters.xml'))
                    experiment_name = run_parameters.getElementsByTagName('ExperimentName')[0].firstChild.nodeValue
                    if run_parameters.getElementsByTagName('ReagentKitSerial'):  # NextSeq
                        lims_container_name = run_parameters.getElementsByTagName('ReagentKitSerial')[0].firstChild.nodeValue
                    elif run_parameters.getElementsByTagName('LibraryTubeSerialBarcode'):  # NovaSeq
                        lims_container_name = run_parameters.getElementsByTagName('LibraryTubeSerialBarcode')[0].firstChild.nodeValue

                    if not os.path.isfile(sample_sheet):
                        getSampleSheet(lims, lims_container_name, sample_sheet)

                    project_name = getProjectName(sample_sheet)
                    if os.path.isfile(sample_sheet):
                        # Lock run folder for conversion
                        os.system('touch {}'.format(conversion_running))

                        # Convert bcl
                        exit_code = convertBCL(run_dir, conversion_log, conversion_error, missing_bcl, barcode_mismatches, fastq_for_index, short_reads)

                        if exit_code == 0:  # Conversion completed
                            # Add flowcell_id to fastq.gz files
                            addFlowcellToFastq(run_dir, flowcell_id)

                            # Generate md5sums for fastq.gz files
                            md5sumFastq(run_dir)

                            # Zip conversion report
                            zip_file = zipConversionReport(run_dir, run_name)

                            # Generate run stats + plots
                            generateRunStats(run_dir, run_name)

                            # Remove lock and set conversion done
                            os.system('date >> {}'.format(conversion_log))
                            os.system('touch {}'.format(conversion_done))
                            os.system('rm {}'.format(conversion_running))

                        else:  # Conversion failed
                            # Remove lock and set conversion done
                            os.system('date >> {}'.format(conversion_log))
                            os.system('touch {}'.format(conversion_failed))
                            os.system('rm {}'.format(conversion_running))

                            # Send failed conversion email
                            conversionFailedMail(run_dir, experiment_name, project_name)



                # Transfer run
                if os.path.isfile(conversion_done) and not os.path.isfile(transfer_running) and not os.path.isfile(transfer_done) and not os.path.isfile(transfer_failed):
                    machine = run_dir.split('/')[-2]
                    transfer_log = open(os.path.join(run_dir, 'illumina_transfer.log'), 'w')
                    transfer_error = open(os.path.join(run_dir, 'illumina_transfer.err'), 'w')

                    rsync_command = '/usr/bin/rsync -rah --update --stats --verbose --prune-empty-dirs'
                    rsync_command += " --include '*/' --include '*.fq.gz' --include '*.fastq.gz' --include 'md5sum.txt' --include 'SampleSheet.csv' --include 'RunInfo.xml' --include '*unParameters.xml' --include 'InterOp/**' --include '*/*/Reports/**' --include 'Data/Intensities/BaseCalls/Stats/*' --include '*.[pP][eE][dD]'"
                    rsync_command += " --exclude '*'"
                    rsync_command += " {0}".format(run_dir)
                    rsync_command += " {0}/{1}".format(DATA_DIR_HPC, machine)

                    # Start transfer process
                    try:
                        os.system('touch {}'.format(transfer_running))
                        transfer_process = subprocess.Popen(rsync_command, stdout=transfer_log, stderr=transfer_error, shell=True)
                        transfers[run_dir] = transfer_process
                    except:
                        transfer_error.write("An error occurred during transfer!\n")

                    transfer_log.close()
                    transfer_error.close()
                #Archive run
                if os.path.isfile(transfer_done) and not os.path.isfile(archive_running)  and not os.path.isfile(archive_done) and not os.path.isfile(archive_failed):
                    machine = run_dir.split('/')[-2]
                    archive_log = open(os.path.join(run_dir, 'illumina_archive.log'), 'w')
                    archive_error = open(os.path.join(run_dir, 'illumina_archive.err'), 'w')

                    try:
                        os.system('touch {}'.format(archive_running))
                        rsync_command = "rsync -rahm --exclude '*fastq.gz' --exclude '*fq.gz' {0} {1}/{2}".format(run_dir, ARCHIVE_DIR, machine)
                        archive_process = subprocess.Popen( rsync_command, stdout=archive_log, stderr=archive_error, shell=True )
                        archive_transfers[run_dir] = archive_process

                    except Exception as e:
                        archive_error.write("Failed running rsync of {0} to {1} with error {2}\n".format(run_dir, ARCHIVE_DIR, e))


    except Exception as e:
        logger.exception('Unhandled Exception')
        print str(e)

    try:
        # Complete transfers
        for run_dir in transfers:
            print 'Transfer: {}'.format(run_dir)
            transfers[run_dir].wait()   # wait for transfer to finished
            run_name = os.path.split(run_dir)[-1]

            transfer_error = os.path.join(run_dir, 'illumina_transfer.err')
            transfer_log = os.path.join(run_dir, 'illumina_transfer.log')
            transfer_running = os.path.join(run_dir, 'TransferRunning.txt')
            transfer_done = os.path.join(run_dir, 'TransferDone.txt')
            transfer_failed = os.path.join(run_dir, 'TransferFailed.txt')
            run_parameters = xml.dom.minidom.parse(os.path.join(run_dir,'RunParameters.xml'))
            experiment_name = run_parameters.getElementsByTagName('ExperimentName')[0].firstChild.nodeValue
            sample_sheet = os.path.join(run_dir, 'SampleSheet.csv')
            project_name = getProjectName(sample_sheet)

            if os.stat(transfer_error).st_size == 0:
                os.system('touch {}'.format(transfer_done))
                os.system('rm {}'.format(transfer_running))

                # Email conversion done

                run_parameters = xml.dom.minidom.parse(os.path.join(run_dir,'RunParameters.xml'))
                experiment_name = run_parameters.getElementsByTagName('ExperimentName')[0].firstChild.nodeValue
                sample_sheet = os.path.join(run_dir, 'SampleSheet.csv')
                project_name = getProjectName(sample_sheet)

                conversionSuccesMail(run_dir, experiment_name, project_name)

                # rsync TransferDone.txt
                machine_run = '/'.join(run_dir.split('/')[-2:])
                rsync_command = "/usr/bin/rsync -h --stats --verbose {0} {1}/{2}/ >> {3} 2>> {4}".format(transfer_done,DATA_DIR_HPC,machine_run,transfer_log,transfer_error)
                os.system(rsync_command)
            else:
                os.system('touch {}'.format(transfer_failed))
                os.system('rm {}'.format(transfer_running))

                transferFailedMail(run_dir, experiment_name, project_name)
    except Exception as e:
        logger.exception('Unhandled Exception')
        print str(e)

    try:
        #Complete archive transfers
        for run_dir in archive_transfers:
            print 'Archiving: {}'.format(run_dir)
            archive_transfers[run_dir].wait()   # wait for transfer to finished
            run_name = os.path.split(run_dir)[-1]

            # archive_error = os.path.join(run_dir, 'illumina_archive.err')
            archive_error = os.path.join(run_dir, 'illumina_archive.err')
            archive_log = os.path.join(run_dir, 'illumina_archive.log')
            archive_running = os.path.join(run_dir, 'ArchiveRunning.txt')
            archive_done = os.path.join(run_dir, 'ArchiveDone.txt')
            archive_failed = os.path.join(run_dir, 'ArchiveFailed.txt')

            if os.stat(archive_error).st_size == 0:
                os.system('touch {}'.format(archive_done))
                os.system('rm {}'.format(archive_running))

                try :
                    for runroot,subdir,runfiles in os.walk(os.path.join(run_dir,'Data/Intensities/BaseCalls')):
                        for file in runfiles:
                            if file.endswith('.fastq.gz') or file.endswith('.fq.gz'):
                                os.remove(os.path.join(runroot,file))
                except Exception as e:
                    # archive_error.write("Failed cleaning up FastQ files for run {0} with error {1}\n".format(run_dir,e))
                    archiveFailedMail(run_dir, experiment_name, project_name)

            else:
                os.system('touch {}'.format(archive_failed))
                os.system('rm {}'.format(archive_running))

                archiveFailedMail(run_dir, experiment_name, project_name)
    except Exception as e:
        logger.exception('Unhandled Exception')
        print str(e)

def run(lims, missing_bcl, barcode_mismatches, fastq_for_index, short_reads):
    """Runs the manageRuns function"""

    manageRuns(lims, missing_bcl, barcode_mismatches, fastq_for_index, short_reads)
