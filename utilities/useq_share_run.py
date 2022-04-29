from genologics.entities import Project
from config import RUN_PROCESSES, RAW_DIR, PROCESSED_DIR, NEXTCLOUD_HOST,NEXTCLOUD_WEBDAV_ROOT,NEXTCLOUD_RAW_DIR,NEXTCLOUD_PROCESSED_DIR,NEXTCLOUD_MANUAL_DIR,MAIL_SENDER, NEXTCLOUD_USER, NEXTCLOUD_PW,DATA_DIRS_RAW,SMS_SERVER,NEXTCLOUD_DATA_ROOT,FILE_STORAGE, SQLALCHEMY_DATABASE_URI,WEBHOST,DATA_DIRS_NANOPORE
from texttable import Texttable
import datetime
import os
import multiprocessing
import subprocess
import time
from modules.useq_nextcloud import NextcloudUtil
from modules.useq_mail import sendMail
from modules.useq_template import TEMPLATE_PATH,TEMPLATE_ENVIRONMENT,renderTemplate
from itertools import islice
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from xml.dom.minidom import parse

import sys
import tarfile
from pathlib import Path
import re
import csv
import glob
import json



def parseConversionStats(lims, dir, pid):
    demux_stats = Path(f'{dir}/Conversion/Reports/Demultiplex_Stats.csv')
    qual_metrics = Path(f'{dir}/Conversion/Reports/Quality_Metrics.csv')
    if not demux_stats.is_file():
        print(f'Warning : Could not find {demux_stats} file.')
        return None

    if not qual_metrics.is_file():
        print(f'Warning : Could not find {qual_metrics} file.')
        return None

    samples = lims.get_samples(projectlimsid=pid)
    sample_names = [x.name for x in samples]
    stats = {
        'total_reads' : 0,
        'total_mean_qual' : 0,
        'total_q30' : 0,
        'samples' : [],
    }


    samples_tmp = {}
    with open(demux_stats, 'r') as d:
        csv_reader = csv.DictReader(d)
        for row in csv_reader:

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
            # stats['samples'].append(row)

    qual_metrics_rows = 0
    with open(qual_metrics,'r') as q:
        csv_reader = csv.DictReader(q)
        for row in csv_reader:
            qual_metrics_rows += 1
            mqs = f'Read {row["ReadNumber"]} Mean Quality Score (PF)'
            q30 = f'Read {row["ReadNumber"]} % Q30'
            if mqs not in samples_tmp[ row['SampleID'] ]:
                samples_tmp[ row['SampleID'] ][mqs] = 0
            if q30 not in samples_tmp[ row['SampleID'] ]:
                samples_tmp[ row['SampleID'] ][q30] = 0

            samples_tmp[ row['SampleID'] ][mqs] += float(row['Mean Quality Score (PF)'])
            samples_tmp[ row['SampleID'] ][q30] += float(row['% Q30'])
            stats['total_q30'] += float(row['% Q30'])
            stats['total_mean_qual'] += float(row['Mean Quality Score (PF)'])

    stats['total_q30'] = (stats['total_q30']/qual_metrics_rows) * 100
    stats['total_mean_qual'] = stats['total_mean_qual'] / qual_metrics_rows

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

def shareManual(researcher,dir):
    name = multiprocessing.current_process().name
    print (f"{name}\tStarting")
    run_zip = Path(f"{dir}/{dir.name}.tar")
    zip_done = Path(f"{dir}/{dir.name}.tar.done")
    if run_zip.is_file() and zip_done.is_file():
        print (f"{name}\tSkipping compression step. {run_zip} and {zip_done} found. ")
    else:
        print (f"{name}\tRunning compression")
        zip_command = f"cd {dir.parents[0]} && tar -chf {run_zip} {dir.name} "
        exit_code = os.system(zip_command)
        if exit_code:
            print (f"Error: Failed creating {run_zip}.")
            return
        else:
            zip_done.touch()



    print (f"{name}\tRunning upload to NextCloud")
    transfer_log = f'{dir}/transfer.log'
    transfer_error = f'{dir}/transfer.error'
    transfer_command = f'scp {run_zip} {NEXTCLOUD_HOST}:{NEXTCLOUD_DATA_ROOT}/{NEXTCLOUD_MANUAL_DIR} 1>>{transfer_log} 2>>{transfer_error}'

    exit_code = os.system(transfer_command)
    if exit_code:
        print (f"{name}\tError : Failed to upload {run_zip} . Please look at {transfer_error} for the reason.")
        return

    time.sleep(90)

    print (f"{name}\tSharing dir {dir} with {researcher.email}")
    share_response = nextcloud_util.share(run_zip.name, researcher.email)
    if "ERROR" in share_response:
        print (f"{name}\tError : Failed to share {run_zip} with message:\n\t{share_response['ERROR']}")
        return
    else:
        share_id = share_response["SUCCES"][0]
        pw = share_response["SUCCES"][1]

        template_data = {
            'dir' : dir.name,
            'nextcloud_host' : NEXTCLOUD_HOST,
            'share_id' : share_id,
            'phone' : researcher.phone
        }

        mail_content = renderTemplate('share_manual_template.html', template_data)
        mail_subject = "USEQ has shared a file with you."

        sendMail(mail_subject,mail_content, MAIL_SENDER ,researcher.email)
        # sendMail(mail_subject,mail_content, MAIL_SENDER ,'s.w.boymans@umcutrecht.nl')
        # print (pw)
        # print()
        os.system(f"ssh usfuser@{SMS_SERVER} \"sendsms.py -m 'Dear_{researcher.username},_A_link_for_{dir.name}_was_send_to_{researcher.email}._{pw}_is_needed_to_unlock_the_link._Regards,_USEQ' -n {researcher.phone}\"")
        run_zip.unlink()
        zip_done.unlink()

    return


def chunkify(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def check(  ):
    yes = set(['yes','y'])
    no = set(['no','n'])
    choice = input().lower()
    if choice in yes:
       choice = True
    elif choice in no:
       choice = False
    else:
       sys.stdout.write("Please respond with '(y)es' or '(n)o'")
    return choice

def getNanoporeRunDetails( lims, project_id, fid ):
    runs = {}
    for root_dir in DATA_DIRS_NANOPORE:

        for summary_file in glob.glob(f"{root_dir}/**/final_summary_*txt", recursive=True):
            parent_dir = Path(summary_file).parent
            with open(summary_file, 'r') as s:
                tmp = {}
                for line in s.readlines():
                    name,val = line.rstrip().split("=")
                    tmp[name]=val
                if 'protocol_group_id' in tmp and project_id in tmp['protocol_group_id']:
                    stats_pdf_search = glob.glob(f"{parent_dir}/*pdf")
                    run_date = datetime.datetime.strptime(tmp['started'].split("T")[0],"%Y-%m-%d")
                    if stats_pdf_search:
                        runs[ run_date ] = {
                            'flowcell_id' : tmp['flow_cell_id'],
                            'run_dir' : parent_dir,
                            'stats_pdf' : stats_pdf_search[0],
                            'date' : run_date
                        }
                    else:
                        runs[ run_date ] = {
                            'flowcell_id' : tmp['flow_cell_id'],
                            'run_dir' : parent_dir,
                            'stats_pdf' : None,
                            'date' : run_date
                        }

    if not runs:
        return None


    latest_date = sorted(runs.keys())[-1]
    return runs[latest_date]

def getIlluminaRunDetails( lims, project_name, fid ):
    """Get the most recent raw run info based on project name and allowed RUN_PROCESSES"""


    runs = {}
    project_processes = lims.get_processes(
        projectname=project_name,
        type=RUN_PROCESSES
    )

    for process in project_processes:
        flowcell_id = None

        if fid:
            flowcell_id = fid
        elif 'Flow Cell ID' in process.udf:
            flowcell_id = process.udf['Flow Cell ID']
        runs[ process.date_run ] = flowcell_id

    if not runs:
        return None

    run_dates = [datetime.datetime.strptime(ts, "%Y-%m-%d") for ts in runs.keys()]
    sorted_run_dates = [datetime.datetime.strftime(ts, "%Y-%m-%d") for ts in sorted(run_dates)]
    latest_flowcell_id = runs[sorted_run_dates[-1]] #the most recent run, this is the run we want to share

    #Try to determine run directory
    for machine_dir in DATA_DIRS_RAW:
        md_path = Path(machine_dir)
        for run_dir in md_path.glob("*"):
            if run_dir.name.endswith("_000000000-"+latest_flowcell_id): #MiSeq
                return (Path(run_dir),latest_flowcell_id,sorted_run_dates[-1])
            elif run_dir.name.endswith("_"+latest_flowcell_id): #NextSeq
                return (Path(run_dir),latest_flowcell_id,sorted_run_dates[-1])
            elif run_dir.name.endswith("A"+latest_flowcell_id) or run_dir.name.endswith("B"+latest_flowcell_id): #HiSeq
                return (Path(run_dir),latest_flowcell_id,sorted_run_dates[-1])

    return None

def shareDataByUser(lims, username, dir):
    #check if username belongs to a valid researcher
    researcher = lims.get_researchers(username=username)

    data_dir = Path(dir)
    if not researcher or not data_dir.is_dir():
        print(f"Error: Either username {username} or directory {data_dir} does not exist.")
        sys.exit()
    researcher = researcher[0]
    if not researcher.phone:
        print(f"Error: {username} has not provided a mobile phone number yet.")

    #check if directory contains known samples belonging to username
    possible_samples = {}
    lims_samples = None
    print (f"Trying to find known samples in {dir}")
    for file in data_dir.rglob("*"):
        # print (file)
        if file.name.endswith('.bam') or file.name.endswith('.fastq.gz') :
            sample_name = file.name.split("_")[0]
            if sample_name not in possible_samples:
                possible_samples[sample_name] = []
            possible_samples[sample_name].append(file)

    print (f"Found {len(possible_samples.keys())} possible samples in {dir}.")

    if len(possible_samples.keys()):
        print (f"Trying to link samples to existing projectIDs.")
        sample_chunks = chunkify(list(possible_samples.keys()), 100)
        lims_samples = []
        for chunk in sample_chunks:
            lims_samples.extend(lims.get_samples(name=chunk))
        # lims_samples = lims.get_samples( name=list(possible_samples.keys()) )
        lims_projects = {}
        for sample in lims_samples:

            sample_project = sample.project
            # print(sample_project)
            user = None
            try:
                user = sample_project.researcher.username
            except:
                user = 'NA'
            id = f"{sample_project.id}:{user}"

            if id not in lims_projects:
                lims_projects[id] = 0
            lims_projects[id] +=1
        print ("Matched samples to the following projectIDs & users:")
        table = Texttable(max_width=0)
        table.add_rows([['projectID','Username','Nr. Samples']])
        for id in lims_projects:
            (projectID, user) = id.split(":")
            table.add_row([projectID, user, lims_projects[id]])
        print (table.draw())
    else:
        print (f"Found no valid samples in {dir}. Trying to match {data_dir.name} to an existing projectID.")
        projectID_matches = re.match("(^\w{3}\d{,5}).*",data_dir.name)
        project =None
        try:
            project = Project(lims, id=projectID_matches.groups()[0])
            id = project.id
            print(f"Match found for {projectID_matches.groups()[0]}")
            table = Texttable(max_width=0)
            table.add_rows([['Project ID','Project Name','Username']])
            table.add_row([project.id,project.name, project.researcher.username])
            print (table.draw())
        except:
            print (f"No project found for projectID {projectID_matches.groups()[0]}")

    print (f"Are you sure you want to share {dir} with {username} ? ")
    if check():
        share_processes = []
        share_process = multiprocessing.Process(name=f"Process_{data_dir.name}", target=shareManual, args=(researcher, data_dir))
        share_processes.append(share_process)
        share_process.start()

        for process in share_processes:
            process.join()


def shareDataById(lims, project_id, fid, link_portal):

    run_info = {}
    project = None
    project_name = ''

    try:
        project = Project(lims, id=project_id)
        project_name = project.name
    except:
        sys.exit(f"Error : Project ID {project_id} not found in LIMS!")

    researcher = project.researcher
    if not researcher.phone:
        sys.exit(f'Error : User ID {researcher.username} for project ID {project_id} has not provided a phone number yet!')

    #Fetch project from Portal DB
    portal_run = session.query(Run).filter_by(run_id=project_id).first()
    if not portal_run:
        sys.exit(f'Error : Project ID {project_id} not found in Portal DB!')



    if portal_run.platform == 'Oxford Nanopore':
        run_info = getNanoporeRunDetails(lims, project_id, fid)

        run_dir = run_info['run_dir']
        if not run_info:
            sys.exit(f'Error : No Nanopore run directory could be found!')

        if nextcloud_util.checkExists( f'{project_id}' ):
            sys.exit(f'Error : {project_id} was already uploaded to Nextcloud, please delete it first!')

        print ("\nAre you sure you want to send the following dataset (yes/no): ")
        table = Texttable(max_width=0)
        table.add_rows([['Data','Project (ID:Name)','Client Email']])
        table.add_row( [ run_dir.name, f"{project_id}:{project_name}", researcher.email])
        print (table.draw())
        if check():
            fast5_pass_dir = Path(f"{run_dir}/fast5_pass")
            fast5_fail_dir = Path(f"{run_dir}/fast5_fail")
            fastq_pass_dir = Path(f"{run_dir}/fastq_pass")
            fastq_fail_dir = Path(f"{run_dir}/fastq_fail")


            barcode_dirs = [x for x in fast5_pass_dir.iterdir() if x.is_dir() and 'barcode' in x.name or 'unclassified' in x.name ]
            upload_dir = Path(f"{run_dir}/{project_id}")
            upload_dir_done = Path(f"{run_dir}/{project_id}.done")
            upload_dir_done.touch()
            upload_dir.mkdir()
            if barcode_dirs:
                for bd in barcode_dirs:
                    zip_command = f"cd {run_dir} && tar -czf {upload_dir}/{bd.name}.tar.gz fast5_fail/{bd.name} fast5_pass/{bd.name} fastq_fail/{bd.name} fastq_pass/{bd.name}"
                    exit_code= os.system(zip_command)
                    if exit_code:
                        sys.exit(f"Error : Failed to create zip file {upload_dir}/{bd.name}.tar.gz.")
                    break

            else:
                fast5_pass_dir = Path(f"{run_dir}/fast5_pass")
                zip_command = f"cd {run_dir} && tar -czf {upload_dir}/{fast5_pass_dir.name}.tar.gz {fast5_pass_dir}"
                exit_code= os.system(zip_command)
                if exit_code:
                    sys.exit(f"Error : Failed to create zip file {upload_dir}/{fast5_pass_dir.name}.tar.gz.")

                fast5_fail_dir = Path(f"{run_dir}/fast5_fail")
                zip_command = f"cd {run_dir} && tar -czf {upload_dir}/{fast5_fail_dir.name}.tar.gz {fast5_fail_dir}"
                exit_code= os.system(zip_command)
                if exit_code:
                    sys.exit(f"Error : Failed to create zip file {upload_dir}/{fast5_fail_dir.name}.tar.gz.")

                fastq_pass_dir = Path(f"{run_dir}/fastq_pass")
                zip_command = f"cd {run_dir} && tar -czf {upload_dir}/{fastq_pass_dir.name}.tar.gz {fastq_pass_dir}"
                exit_code= os.system(zip_command)
                if exit_code:
                    sys.exit(f"Error : Failed to create zip file {upload_dir}/{fastq_pass_dir.name}.tar.gz.")

                fastq_fail_dir = Path(f"{run_dir}/fastq_fail")
                zip_command = f"cd {run_dir} && tar -czf {upload_dir}/{fastq_fail_dir.name}.tar.gz {fastq_fail_dir}"
                exit_code= os.system(zip_command)
                if exit_code:
                    sys.exit(f"Error : Failed to create zip file {upload_dir}/{fastq_fail_dir.name}.tar.gz.")

            zip_stats = f"cd {run_dir} && tar -czf {upload_dir}/stats.tar.gz report_*.pdf"
            exit_code = os.system(zip_stats)
            if exit_code:
                sys.exit(f"Error : Failed to create zip file {upload_dir}/stats.tar.gz.")

            print (f"Running upload to NextCloud")
            transfer_command = f'scp -r {upload_dir} {upload_dir_done} {NEXTCLOUD_HOST}:{NEXTCLOUD_DATA_ROOT}/{NEXTCLOUD_RAW_DIR}'
            exit_code = os.system(transfer_command)
            if exit_code:
                sys.exit(f"Error : Failed to upload {upload_dir} to NextCloud.")
#

            time.sleep(90)

            print (f"Sharing dir {run_dir} with {researcher.email}")
            share_response = nextcloud_util.share(project_id, researcher.email)
            if "ERROR" in share_response:
                sys.exit (f"Error : Failed to share {project_id} with message:\n\t{share_response['ERROR']}")

            else:
                share_id = share_response["SUCCES"][0]
                pw = share_response["SUCCES"][1]
                template_data = {
                    'project_id' : project_id,
                    'phone' : researcher.phone,
                    'nextcloud_host' : NEXTCLOUD_HOST,
                    'share_id' : share_id,
                }
                mail_content = renderTemplate('share_nanopore_template.html', template_data)
                mail_subject = f"USEQ sequencing of sequencing-run ID {project_id} finished"

                sendMail(mail_subject,mail_content, MAIL_SENDER ,researcher.email)
                os.system(f"ssh usfuser@{SMS_SERVER} \"sendsms.py -m 'Dear_{researcher.username},_A_link_for_runID_{project_id}_was_send_to_{researcher.email}._{pw}_is_needed_to_unlock_the_link._Regards,_USEQ' -n {researcher.phone}\"")
                # sendMail(mail_subject,mail_content, MAIL_SENDER ,'s.w.boymans@umcutrecht.nl')
                # print (pw)

            os.system(f'rm -r {upload_dir} {upload_dir_done}')

            prev_results = session.query(NanoporeSequencingStats).filter_by(flowcell_id=run_info['flowcell_id']).first()
            if prev_results:
                sys.exit(f'Warning : Stats for {run_info["flowcell_id"]} where already uploaded to portal db. Skipping.')

            tmp_dir = Path(f"{run_dir}/{run_info['flowcell_id']}")
            if not tmp_dir.is_dir():
                tmp_dir.mkdir()
            os.system(f"cp {run_info['stats_pdf']} {tmp_dir}")

            rsync_command = f"/usr/bin/rsync -rah {tmp_dir} {WEBHOST}:{FILE_STORAGE}/"

            exit_code = os.system(rsync_command)
            if exit_code :
                sys.exit(f'Error : Failed tot copy stats pdf to {FILE_STORAGE}. Please fix this manually using link_run_results!')

            os.system(f"rm -r {tmp_dir}")
            nan_stats = NanoporeSequencingStats(
                general_stats = Path(run_info['stats_pdf']).name,
                date = run_info['date'],
                flowcell_id = run_info['flowcell_id'],
                run_id=portal_run.id
            )
            session.add(nan_stats)
            session.commit()
            print(f'Uploaded stats for {project_id} to portal db.')
    else:

        run_dir, flowcell_id, date_run = getIlluminaRunDetails(lims, project_name, fid)
        if not run_dir:
            sys.exit(f'Error : No Illumina run directory could be found!')

        if not nextcloud_util.checkExists( f'{project_id}' ):
            sys.exit(f'Error : {project_id} was not uploaded to Nextcloud yet!')

        file_list = nextcloud_util.simpleFileList(project_id)
        if not file_list:
            sys.exit(f"{name}\tError : No files found in nextcloud dir  {project_id}!")

        conversion_stats = parseConversionStats(lims, run_dir ,project_id )
        if not conversion_stats:
            print(f'Warning : No conversion stats could be found for {run_dir}!')



        print ("\nAre you sure you want to send the following datasets (yes/no): ")
        table = Texttable(max_width=0)
        table.add_rows([['Data','Project (ID:Name)','Client Email', '# Samples','Total Reads', '% Q30', 'Mean Quality']])
        if conversion_stats:
            table.add_row( [ run_dir.name, f"{project_id}:{project_name}", researcher.email, len(conversion_stats['samples']), conversion_stats['total_reads'],f"{conversion_stats['total_q30']}",conversion_stats['total_mean_qual']])
        else:
            table.add_row( [ run_dir.name, f"{project_id}:{project_name}", researcher.email, '?','?','?','?'])
        print (table.draw())

        if check():
            share_response = nextcloud_util.share(project_id, researcher.email)

            if "ERROR" in share_response:
                sys.exit (f"{name}\tError : Failed to share {project_id} with message:\n\t{share_response['ERROR']}")
            else:
                share_id = share_response["SUCCES"][0]
                pw = share_response["SUCCES"][1]

                template_data = {
                    'project_id' : project_id,
                    'phone' : researcher.phone,
                    'nextcloud_host' : NEXTCLOUD_HOST,
                    'share_id' : share_id,
                    'file_list' : file_list,
                    'conversion_stats' : conversion_stats
                }

                mail_content = renderTemplate('share_illumina_template.html', template_data)
                mail_subject = f"USEQ sequencing of sequencing-run ID {project_id} finished"

                sendMail(mail_subject,mail_content, MAIL_SENDER ,researcher.email)
                os.system(f"ssh usfuser@{SMS_SERVER} \"sendsms.py -m 'Dear_{researcher.username},_A_link_for_runID_{project_id}_was_send_to_{researcher.email}._{pw}_is_needed_to_unlock_the_link._Regards,_USEQ' -n {researcher.phone}\"")

                # sendMail(mail_subject,mail_content, MAIL_SENDER ,'s.w.boymans@umcutrecht.nl')
                # print (pw)

                print(f'Shared {project_id} with {researcher.email}')
                # session = Session(engine)

                prev_results = session.query(IlluminaSequencingStats).filter_by(flowcell_id=flowcell_id).first()
                if prev_results:
                    sys.exit(f'Warning : Stats for {flowcell_id} where already uploaded to portal db. Skipping.')

                if conversion_stats:
                    tmp_dir = Path(f"{run_dir}/{flowcell_id}")
                    if not tmp_dir.is_dir():
                        tmp_dir.mkdir()
                    conversion_stats_json_file = Path(f"{tmp_dir}/Conversion_Stats.json")
                    with open(conversion_stats_json_file, 'w') as c:
                        c.write( json.dumps( conversion_stats['samples'] ) )

                    os.system(f"scp -r {run_dir}/Conversion/Reports/*png {tmp_dir}")

                    rsync_command = f"/usr/bin/rsync -rah {tmp_dir} {WEBHOST}:{FILE_STORAGE}/"
                    exit_code = os.system(rsync_command)
                    if exit_code :
                        sys.exit(f'Error : Failed tot copy plots to {FILE_STORAGE}. Please fix this manually using link_run_results!')

                    ill_stats = IlluminaSequencingStats(
                        flowcell_id=flowcell_id,
                        general_stats=conversion_stats_json_file.name,
                        date=date_run,
                        flowcell_intensity_plot = f'{run_dir.name}_flowcell-Intensity.png',
                        flowcell_density_plot = f'{run_dir.name}_Clusters-by-lane.png',
                        total_qscore_lanes_plot = f'{run_dir.name}_q-histogram.png',
                        cycle_qscore_lanes_plot = f'{run_dir.name}_q-heat-map.png',
                        cycle_base_plot = f'{run_dir.name}_BasePercent-by-cycle_BasePercent.png',
                        cycle_intensity_plot = f'{run_dir.name}_Intensity-by-cycle_Intensity.png',
                        run_id=portal_run.id
                    )
                    session.add(ill_stats)
                    session.commit()
                    print(f'Uploaded stats for {project_id} to portal db.')
                else:
                    ill_stats = IlluminaSequencingStats(
                        flowcell_id=flowcell_id,
                        date=date_run,
                        run_id=portal_run.id
                    )
                    print(f'Uploaded only flowcell_id for {project_id} to portal db.')
                    session.add(ill_stats)
                    session.commit()




def run(lims, ids, username, dir, fid, link_portal):
    """Runs raw, processed or manual function based on mode"""

    global nextcloud_util
    global session
    global Run
    global IlluminaSequencingStats
    global NanoporeSequencingStats

    #Set up portal db connection +
    Base = automap_base()
    ssl_args = {'ssl_ca': '/etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt'}
    engine = create_engine(SQLALCHEMY_DATABASE_URI, connect_args=ssl_args, pool_pre_ping=True)
    Base.prepare(engine, reflect=True)
    Run = Base.classes.run
    IlluminaSequencingStats = Base.classes.illumina_sequencing_stats
    NanoporeSequencingStats = Base.classes.nanopore_sequencing_stats
    session = Session(engine)

    #Set up nextcloud
    nextcloud_util = NextcloudUtil()
    nextcloud_util.setHostname( NEXTCLOUD_HOST )

    if ids:
        nextcloud_util.setup( NEXTCLOUD_USER, NEXTCLOUD_PW, NEXTCLOUD_WEBDAV_ROOT,NEXTCLOUD_RAW_DIR,MAIL_SENDER )
        shareDataById(lims, ids, fid, link_portal)

    elif username and dir:
        nextcloud_util.setup( NEXTCLOUD_USER, NEXTCLOUD_PW, NEXTCLOUD_WEBDAV_ROOT,NEXTCLOUD_MANUAL_DIR,MAIL_SENDER )
        shareDataByUser(lims, username, dir)
