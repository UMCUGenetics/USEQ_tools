from genologics.entities import Project
from config import RUN_PROCESSES, RAW_DIR, PROCESSED_DIR, NEXTCLOUD_HOST,NEXTCLOUD_WEBDAV_ROOT,NEXTCLOUD_RAW_DIR,NEXTCLOUD_PROCESSED_DIR,NEXTCLOUD_MANUAL_DIR,MAIL_SENDER, NEXTCLOUD_USER, NEXTCLOUD_PW,DATA_DIRS_RAW,SMS_SERVER,NEXTCLOUD_DATA_ROOT
from texttable import Texttable
import datetime
import os
import multiprocessing
import subprocess
import time
from modules.useq_illumina_parsers import getExpectedReads
from modules.useq_nextcloud import NextcloudUtil
from modules.useq_mail import sendMail
from modules.useq_template import TEMPLATE_PATH,TEMPLATE_ENVIRONMENT,renderTemplate
from itertools import islice
import sys
import tarfile
from pathlib import Path
import re
import csv
import glob

def parseConversionStats(lims, dir, pid):
    demux_stats = f'{dir}/Conversion/Reports/Demultiplex_Stats.csv'
    top_unknown = f'{dir}/Conversion/Reports/Top_Unknown_Barcodes.csv'
    qual_metrics = f'{dir}/Conversion/Reports/Quality_Metrics.csv'

    samples = lims.get_samples(projectlimsid=pid)
    sample_names = [x.name for x in samples]
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
            # stats['samples'].append(row)
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

    with open(top_unknown, 'r') as t:
        csv_reader = csv.DictReader(t)
        for row in islice(csv_reader,0,20):
            stats['top_unknown'].append(row)
    return stats

def zipRun( dir, dir_info=None):
    run_name = os.path.basename(dir)
    zip_name = None
    name = multiprocessing.current_process().name

    if dir_info:
        zip_name = "-".join(dir_info['projects'].keys())
    else:
        zip_name = os.path.basename(dir)

    run_zip = "{0}/{1}.tar".format(dir,zip_name)
    if os.path.isfile(run_zip) and os.path.isfile(f"{run_zip}.done"):
        print (f"{name}\tSkipping compression step. {run_zip} and {run_zip}.done found. ")
    else:
        with tarfile.open(run_zip, "w", dereference=True) as tar:
            tar.add(dir, arcname=run_name)
        open(f'{run_zip}.done', 'w').close()
    return run_zip


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
    #upload_response = nextcloud_util.upload(run_zip)
    #if "ERROR" in upload_response:
    #    print (f"{name}\tError : Failed to upload {run_zip} with message:\n\t{upload_response['ERROR']}")
    #    return
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

def shareRaw(lims, project_id,project_info, link_portal):

    name = multiprocessing.current_process().name
    print (f"{name}\tStarting")

    conversion_stats = None
    if Path(f'{project_info["dir"]}/Conversion/Reports/Demultiplex_Stats.csv').is_file():
        conversion_stats = parseConversionStats(lims, project_info['dir'] ,project_id )



    expected_yield = getExpectedReads( f"{project_info['dir']}/RunParameters.xml" )
    if not expected_yield:
        print (f"{name}\tError : No RunParameters.xml file could be found in {project_info['dir']}!")
        return

    file_list = nextcloud_util.simpleFileList(project_id)
    if not file_list:
        print (f"{name}\tError : No files found in nextcloud dir  {project_id}!")
        return

    print (f"{name}\tSharing run {project_info['data']} with {project_info['researcher'].email}")
    share_response = nextcloud_util.share(project_info['data'], project_info['researcher'].email)
    if "ERROR" in share_response:
        print (f"{name}\tError : Failed to share {project_id} with message:\n\t{share_response['ERROR']}")
        return
    else:
        share_id = share_response["SUCCES"][0]
        pw = share_response["SUCCES"][1]
        # print (pw)
        template_data = {
            'project_id' : project_id,
            'phone' : project_info['researcher'].phone,
            'nextcloud_host' : NEXTCLOUD_HOST,
            'share_id' : share_id,
            'file_list' : file_list,
            'conversion_stats' : conversion_stats
        }

        mail_content = renderTemplate('share_raw_template.html', template_data)
        mail_subject = f"USEQ sequencing of sequencing-run ID {project_id} finished"

        sendMail(mail_subject,mail_content, MAIL_SENDER ,'s.w.boymans@umcutrecht.nl')
        print (pw)
        # sendMail(mail_subject,mail_content, MAIL_SENDER ,project_info['researcher'].email)
        # os.system(f"ssh usfuser@{SMS_SERVER} \"sendsms.py -m 'Dear_{project_info['researcher'].username},_A_link_for_runID_{project_id}_was_send_to_{project_info['researcher'].email}._{pw}_is_needed_to_unlock_the_link._Regards,_USEQ' -n {project_info['researcher'].phone}\"")

        if link_portal:
            from sqlalchemy.ext.automap import automap_base
            from sqlalchemy.orm import Session
            from sqlalchemy import create_engine
            from xml.dom.minidom import parse
            from config import FILE_STORAGE, SQLALCHEMY_DATABASE_URI
            run_info = parse(Path(f"{project_info['dir']}/RunInfo.xml"))
            flowcell_id = run_info.getElementsByTagName('Flowcell')[0].firstChild.nodeValue

            with open( Path(f"{project_info['dir']}/Conversion/Reports/multiqc_data/multiqc_bclconvert_bysample.json", 'r') as s:
                general_stats = s.read()
# ssh sitkaspar "mkdir -p /home/cog/sboymans/useq_portal/filestore/test/"
            # os.system("ssh")
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



def getRawData( lims, project_name, fid ):
    """Get the most recent raw run info based on project name and allowed RUN_PROCESSES"""
    runs = {}
    print (project_name)
    project_processes = lims.get_processes(
        projectname=project_name,
        type=RUN_PROCESSES
    )

    for process in project_processes:
        run_id = None
        flowcell_id = None
        print(process)
        if 'Run ID' in process.udf: run_id = process.udf['Run ID']
        if fid:
            flowcell_id = fid
        elif 'Flow Cell ID' in process.udf:
            flowcell_id = process.udf['Flow Cell ID']
        runs[ process.date_run ] = [  run_id, flowcell_id ]
    print(runs)

    if not runs:
        return None

    run_dates = [datetime.datetime.strptime(ts, "%Y-%m-%d") for ts in runs.keys()]
    sorted_run_dates = [datetime.datetime.strftime(ts, "%Y-%m-%d") for ts in sorted(run_dates)]
    recent_run = runs[sorted_run_dates[-1]] #the most recent run, this is the run we want to share

    #Try to determine run directory
    if recent_run[0]:
        for machine_dir in DATA_DIRS_RAW:
            md_path = Path(f'{machine_dir}/{recent_run[0]}')
            if md_path.is_dir():
                return md_path

    elif recent_run[1]: #run flowcell is known
        for machine_dir in DATA_DIRS_RAW:
            md_path = Path(machine_dir)
            for run_dir in md_path.glob("*"):
                if run_dir.name.endswith("_000000000-"+recent_run[1]): #MiSeq
                    return run_dir
                elif run_dir.name.endswith("_"+recent_run[1]): #NextSeq
                    return run_dir
                elif run_dir.name.endswith("A"+recent_run[1]) or run_dir.name.endswith("B"+recent_run[1]): #HiSeq
                    return run_dir

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


def shareDataById(lims, ids, fid, link_portal):
    """Get's the run names, encrypts the run data and sends it to the appropriate client"""
    project_ids = ids.split(",")
    run_info = {}

    for project_id in project_ids:
        project = None
        project_name = ''
        try:
            project = Project(lims, id=project_id)
            project_name = project.name
        except:
            print (f"Error : Project ID {project_id} not found!")
            continue

        researcher = project.researcher
        if not researcher.phone:
            print(f'Error : User ID {researcher.username} for project ID {project_id} has not provided a phone number yet.')
            continue

        run_dir = getRawData(lims, project_name, fid)

        print(run_dir)
        print ( f'{project_id}')
        if not nextcloud_util.checkExists( f'{project_id}' ) or not run_dir:
            print (f'Error : {project_id} was not uploaded to Nextcloud yet.')
            continue

        run_info[project_id] = {

            'researcher' : researcher,
            'project_name' : project_name,
            'data' : f'{project_id}',
            'dir' : run_dir
        }

    if not run_info:
        print ("Error : None of the provided project IDs are able to be processed!")
    else:
        print ("\nAre you sure you want to send the following datasets(s) (yes/no): ")
        table = Texttable(max_width=0)
        table.add_rows([['Data','Project (ID:Name)','Client Email']])
        for project_id in run_info:
            table.add_row( [ run_info[project_id]['data'], f"{project_id}:{run_info[project_id]['project_name']}", run_info[project_id]['researcher'].email])
        print (table.draw())

        if check():
            #Start sharing threads
            share_processes =[]
            for project_id in run_info:
                share_process = None

                share_process = multiprocessing.Process(name=f"Process_{project_id}", target=shareRaw, args=(lims,project_id, run_info[project_id], link_portal) )

                share_processes.append(share_process)
                share_process.start()

            for process in share_processes:
                process.join()

def run(lims, ids, username, dir, fid, link_portal):
    """Runs raw, processed or manual function based on mode"""

    global nextcloud_util

    #Set up nextcloud
    nextcloud_util = NextcloudUtil()
    nextcloud_util.setHostname( NEXTCLOUD_HOST )

    if ids:
        nextcloud_util.setup( NEXTCLOUD_USER, NEXTCLOUD_PW, NEXTCLOUD_WEBDAV_ROOT,NEXTCLOUD_RAW_DIR,MAIL_SENDER )
        shareDataById(lims, ids, fid, link_portal)

    elif username and dir:
        nextcloud_util.setup( NEXTCLOUD_USER, NEXTCLOUD_PW, NEXTCLOUD_WEBDAV_ROOT,NEXTCLOUD_MANUAL_DIR,MAIL_SENDER )
        shareDataByUser(lims, username, dir)
