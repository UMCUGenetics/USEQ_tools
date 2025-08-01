from genologics.entities import Project
from config import Config
from texttable import Texttable
import datetime
import os
import multiprocessing
import time
from modules.useq_nextcloud import NextcloudUtil
from modules.useq_mail import sendMail
from modules.useq_template import TEMPLATE_PATH,TEMPLATE_ENVIRONMENT,renderTemplate
from utilities.useq_sample_report import getSampleMeasurements
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from xml.dom.minidom import parse
import sys
from pathlib import Path
import re
import csv
import glob
import json

def createDBSession():


    #Set up portal db connection +
    Base = automap_base()
    ssl_args = {'ssl_ca': Config.SSL_CERT}
    engine = create_engine(Config.PORTAL_DB_URI, connect_args=ssl_args, pool_pre_ping=True, pool_recycle=21600)

    Base.prepare(engine, reflect=True)
    Run = Base.classes.run
    IlluminaSequencingStats = Base.classes.illumina_sequencing_stats
    NanoporeSequencingStats = Base.classes.nanopore_sequencing_stats
    session = Session(engine)

    return (session,Run, IlluminaSequencingStats,NanoporeSequencingStats)

def parseSummaryStats(dir):
    run_name = dir.name
    summary_stats = Path(f'{dir}/Conversion/Reports/{run_name}_summary.csv')
    if not summary_stats.is_file():
        print(f'Warning : Could not find {summary_stats} file.')
        return None

    stats = {
        'yield_r1' : 0,
        'yield_r2' : 0,
        'reads' : 0,
        'cluster_density' : 0,
        'perc_q30_r1' : 0,
        'perc_q30_r2' : 0,
        'perc_occupied' : 0,
        'phix_aligned' : 0
    }

    with open(summary_stats, 'r') as sumcsv:
        lines = sumcsv.readlines()
        line_nr = 0
        while line_nr < len(lines):
            # print(line_nr)
            line = lines[line_nr].rstrip()
            if not line: line_nr+=1;continue

            if line.startswith('Read') and len(line) == 6:
                read_nr = 2 if stats["reads"] else 1
                total_yield = []
                total_reads = []
                total_density = []
                total_q30 = []
                total_occupied = []
                total_aligned = []

                #Parse stat block for read nr
                sub_counter = 0
                for sub_line in lines[line_nr+2:]:
                    cols = sub_line.split(",")
                    sub_counter += 1
                    # print(cols)
                    if cols[0].rstrip().isdigit():
                        if '-' in cols[1]:

                            total_yield.append(float( cols[11].rstrip() ))
                            total_reads.append(float(cols[9].rstrip() ))
                            total_density.append( int(cols[3].split("+")[0].rstrip() ))
                            total_q30.append(float(cols[10].rstrip() ))
                            total_occupied.append(float(cols[18].split("+")[0].rstrip() ))
                            total_aligned.append(float(cols[13].split("+")[0].rstrip() ))
                    else:
                        sub_counter -= 1
                        break

                line_nr += sub_counter
                #
                stats[f"yield_r{read_nr}"] = round (float(sum(total_yield) ),2)
                stats["reads"] = round(float(sum(total_reads)),2)
                stats["cluster_density"] = round(float(sum(total_density) / len(total_density)),2)
                stats[f"perc_q30_r{read_nr}"] =round(float(sum(total_q30) / len(total_q30)),2)
                stats["perc_occupied"] =round(float(sum(total_occupied) / len(total_occupied)),2)
                stats["phix_aligned"] = round(float(sum(total_aligned) / len(total_aligned)),2)

            else:
                line_nr +=1

    return stats



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
        'avg_quality_r1' : 0,
        'avg_quality_r2' : 0,
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
                    'Lane' : []
                }

            samples_tmp[ row['SampleID'] ]['Index'] = row['Index']
            samples_tmp[ row['SampleID'] ]['Lane'].append(int(row['Lane']))
            samples_tmp[ row['SampleID'] ]['# Reads'] += int(row['# Reads'])
            samples_tmp[ row['SampleID'] ]['# Perfect Index Reads']  += int(row['# Perfect Index Reads'])
            samples_tmp[ row['SampleID'] ]['# One Mismatch Index Reads']  += int(row['# One Mismatch Index Reads'])
            # print(samples_tmp[ row['SampleID'] ])

    qual_metrics_rows = 0
    rows = 0
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

            if row['ReadNumber'] == '1':
                rows +=1
                stats['avg_quality_r1'] += float(row['Mean Quality Score (PF)'])
            elif row['ReadNumber'] == '2':
                stats['avg_quality_r2'] += float(row['Mean Quality Score (PF)'])



            samples_tmp[ row['SampleID'] ][mqs] += float(row['Mean Quality Score (PF)'])
            samples_tmp[ row['SampleID'] ][q30] += float(row['% Q30'])
            stats['total_q30'] += float(row['% Q30'])
            stats['total_mean_qual'] += float(row['Mean Quality Score (PF)'])

    stats['total_q30'] = (stats['total_q30']/qual_metrics_rows) * 100
    stats['total_mean_qual'] = stats['total_mean_qual'] / qual_metrics_rows
    stats['avg_quality_r1'] = round(stats['avg_quality_r1'] / rows, 2)
    stats['avg_quality_r2'] =  round(stats['avg_quality_r2'] / rows,2)
    # print(samples_tmp)
    for sampleID in samples_tmp:

        if sampleID not in sample_names:continue
        sample = {}
        for read_number in ['1','2','I1','I2']:
            if f'Read {read_number} Mean Quality Score (PF)' in samples_tmp[sampleID]:
                sample[f'Read {read_number} Mean Quality Score (PF)'] = samples_tmp[sampleID][f'Read {read_number} Mean Quality Score (PF)'] / len(samples_tmp[ sampleID ]['Lane'])
            if f'Read {read_number} % Q30' in samples_tmp[sampleID]:
                sample[f'Read {read_number} % Q30'] = (samples_tmp[sampleID][f'Read {read_number} % Q30'] / len(samples_tmp[ sampleID ]['Lane']))*100
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
    transfer_command = f'scp {run_zip} {Config.NEXTCLOUD_HOST}:{Config.NEXTCLOUD_DATA_ROOT}/{Config.NEXTCLOUD_MANUAL_DIR} 1>>{transfer_log} 2>>{transfer_error}'

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
            'name' : f"{researcher.first_name} {researcher.last_name}",
            'dir' : dir.name,
            'nextcloud_host' : Config.NEXTCLOUD_HOST,
            'share_id' : share_id,
            'phone' : researcher.phone
        }

        mail_content = renderTemplate('share_manual_template.html', template_data)
        mail_subject = "USEQ has shared a file with you."

        sendMail(mail_subject,mail_content, Config.MAIL_SENDER ,researcher.email)
        # sendMail(mail_subject,mail_content, MAIL_SENDER ,'s.w.boymans@umcutrecht.nl')
        # print (pw)
        # print()
        os.system(f"ssh usfuser@{Config.SMS_SERVER} \"sendsms.py -m 'Dear_{researcher.username},_A_link_for_{dir.name}_was_send_to_{researcher.email}._{pw}_is_needed_to_unlock_the_link._Regards,_USEQ' -n {researcher.phone}\"")
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

    for summary_file in glob.glob(f"{Config.HPC_RAW_ROOT}/nanopore/**/final_summary_*txt", recursive=True):
        parent_dir = Path(summary_file).parent
        with open(summary_file, 'r') as s:
            tmp = {}
            for line in s.readlines():
                name,val = line.rstrip().split("=")
                tmp[name]=val
            if 'protocol_group_id' in tmp and project_id in tmp['protocol_group_id']:
                stats_pdf_search = glob.glob(f"{parent_dir}/*pdf")
                stats_html_search = glob.glob(f"{parent_dir}/*html")
                run_date = datetime.datetime.strptime(tmp['started'].split("T")[0],"%Y-%m-%d")
                if stats_pdf_search:
                    runs[ run_date ] = {
                        'flowcell_id' : tmp['flow_cell_id'],
                        'run_dir' : parent_dir,
                        'stats_file' : stats_pdf_search[0],
                        'date' : run_date
                    }
                elif stats_html_search:
                    runs[ run_date ] = {
                        'flowcell_id' : tmp['flow_cell_id'],
                        'run_dir' : parent_dir,
                        'stats_file' : stats_html_search[0],
                        'date' : run_date
                    }
                else:
                    runs[ run_date ] = {
                        'flowcell_id' : tmp['flow_cell_id'],
                        'run_dir' : parent_dir,
                        'stats_file' : None,
                        'date' : run_date
                    }

    if not runs:
        return None


    latest_date = sorted(runs.keys())[-1]
    return runs[latest_date]

def getIlluminaRunDetails( lims, project_name, fid ):
    """Get the most recent raw run info based on project name and allowed RUN_PROCESSES"""

    # illumina_seq_steps = Config.WORKFLOW_STEPS['SEQUENCING']['steps']['ILLUMINA SEQUENCING']['names'] + ['NextSeq Run (NextSeq) 1.0','MiSeq Run (MiSeq) 4.0','HiSeq Run (HiSeq) 5.0']
    runs = {}
    project_processes = lims.get_processes(
        projectname=project_name,

    )

    for process in project_processes:
        flowcell_id = None
        # print(process.type.name)
        if not process.date_run: continue

        if 'Flow Cell ID' in process.udf:
            if fid :
                flowcell_id = fid
            else:
                flowcell_id = process.udf['Flow Cell ID']

            runs[ process.date_run ] = {
                'flowcell_id' : flowcell_id,
                'date_started' : process.date_run,
                'phix_loaded' : process.parent_processes()[0].udf.get('% PhiX Control', 0),
                'load_conc' : process.input_output_maps[0][0]['uri'].udf.get("Loading Conc. (pM)", 0 )
            }
            #
        elif fid:
            flowcell_id = fid

            runs[ process.date_run ] = {
                'flowcell_id' : flowcell_id,
                'date_started' : datetime.datetime.today(),
                'phix_loaded' : 0,
                'load_conc' : 0
            }

    if not runs:
        return None

    run_dates = [datetime.datetime.strptime(ts, "%Y-%m-%d") for ts in runs.keys()]
    sorted_run_dates = [datetime.datetime.strftime(ts, "%Y-%m-%d") for ts in sorted(run_dates)]
    latest_flowcell_id = runs[sorted_run_dates[-1]]['flowcell_id'] #the most recent run, this is the run we want to share

    #Try to determine run directory
    for machine in Config.MACHINE_ALIASES:
        machine_dir = f"{Config.HPC_RAW_ROOT}/{machine}"
        md_path = Path(machine_dir)
        for run_dir in md_path.glob("*"):
            if run_dir.name.endswith("_000000000-"+latest_flowcell_id): #MiSeq
                return (Path(run_dir),latest_flowcell_id,runs[sorted_run_dates[-1]])
            elif run_dir.name.endswith("_"+latest_flowcell_id): #NextSeq
                return (Path(run_dir),latest_flowcell_id,runs[sorted_run_dates[-1]])
            elif run_dir.name.endswith("A"+latest_flowcell_id) or run_dir.name.endswith("B"+latest_flowcell_id): #HiSeq
                return (Path(run_dir),latest_flowcell_id,runs[sorted_run_dates[-1]])

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


def shareDataById(lims, project_id, fid, all_dirs_ont):

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
    session,Run, IlluminaSequencingStats,NanoporeSequencingStats = createDBSession()

    portal_run = session.query(Run).filter_by(run_id=project_id).first()
    if not portal_run:
        sys.exit(f'Error : Project ID {project_id} not found in Portal DB!')


    sample_measurements = getSampleMeasurements(lims, project_id)

    samples = lims.get_samples(projectlimsid=project_id)



    if project.udf['Application'] == 'ONT Sequencing':
        run_info = getNanoporeRunDetails(lims, project_id, fid)

        run_dir = run_info['run_dir']
        if not run_info:
            sys.exit(f'Error : No Nanopore run directory could be found!')



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
            bam_pass_dir = Path(f"{run_dir}/bam_pass")
            bam_fail_dir = Path(f"{run_dir}/bam_fail")
            pod5_pass_dir = Path(f"{run_dir}/pod5_pass")
            pod5_fail_dir = Path(f"{run_dir}/pod5_fail")
            pod5_dir = Path(f"{run_dir}/pod5")
            barcode_dirs = None
            data_dirs = [fast5_pass_dir,fast5_fail_dir,fastq_pass_dir,fastq_fail_dir,bam_pass_dir,bam_fail_dir,pod5_pass_dir,pod5_fail_dir,pod5_dir,pod5_dir]
            if nextcloud_util.checkExists( f'{project_id}' ):
                print(f'Warning : Deleting previous version of {project_id} on Nextcloud')
                nextcloud_util.delete(project_id)
                nextcloud_util.delete(f'{project_id}.done')


            if fastq_pass_dir.is_dir():
                barcode_dirs = [x for x in fastq_pass_dir.iterdir() if x.is_dir() and 'barcode' in x.name or 'unclassified' in x.name ]

            upload_dir = Path(f"{run_dir}/{project_id}")
            upload_dir_done = Path(f"{run_dir}/{project_id}.done")
            upload_dir_done.touch()
            upload_dir.mkdir()
            file_list = []
            available_files = open(f'{run_dir}/available_files.txt', 'w', newline='\n')
            if barcode_dirs and not all_dirs_ont:
                for bd in barcode_dirs:
                    zip_command = f"cd {run_dir} && tar -czf {upload_dir}/{bd.name}.tar.gz"

                    for data_dir in data_dirs:
                        if Path(f"{data_dir}/{bd.name}").is_dir():
                            zip_command += f" {data_dir.name}/{bd.name}"

                    # print(zip_command)
                    exit_code= os.system(zip_command)
                    if exit_code:
                        sys.exit(f"Error : Failed to create zip file {upload_dir}/{bd.name}.tar.gz.")
                    file_list.append(f"{bd.name}.tar.gz")
                    available_files.write(f"{bd.name}.tar.gz\n")
            else:
                for data_dir in data_dirs:
                    if data_dir.is_dir():
                        zip_command = f"cd {run_dir} && tar -czf {upload_dir}/{data_dir.name}.tar.gz {data_dir.name}"
                        print(zip_command)
                        exit_code= os.system(zip_command)
                        if exit_code:
                            sys.exit(f"Error : Failed to create zip file {upload_dir}/{data_dir.name}.tar.gz.")

                        available_files.write(f"{data_dir.name}.tar.gz\n")
                        file_list.append(f"{data_dir.name}.tar.gz")


            zip_stats = f"cd {run_dir} && tar -czf {upload_dir}/stats.tar.gz other_reports/ *.*"
            exit_code = os.system(zip_stats)

            if exit_code:
                sys.exit(f"Error : Failed to create zip file {upload_dir}/stats.tar.gz.")
            available_files.write("stats.tar.gz\n")
            available_files.close()
            file_list.append("stats.tar.gz")

            print (f"Running upload to NextCloud")
            transfer_command = f'scp -r {upload_dir} {upload_dir_done} {Config.NEXTCLOUD_HOST}:{Config.NEXTCLOUD_DATA_ROOT}/{Config.NEXTCLOUD_RAW_DIR}'
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
                    'name' : f"{researcher.first_name} {researcher.last_name}",
                    'project_id' : project_id,
                    'phone' : researcher.phone,
                    'nextcloud_host' : Config.NEXTCLOUD_HOST,
                    'share_id' : share_id,
                    'file_list' : file_list,
                    'sample_measurements' : sample_measurements
                }
                mail_content = renderTemplate('share_nanopore_template.html', template_data)
                mail_subject = f"USEQ sequencing of sequencing-run ID {project_id} finished"

                if Config.DEVMODE:
                    sendMail(mail_subject,mail_content, Config.MAIL_SENDER ,'s.w.boymans@umcutrecht.nl', attachments={'available_files':f'{run_dir}/available_files.txt'})
                    print (pw)
                else:
                    sendMail(mail_subject,mail_content, Config.MAIL_SENDER ,researcher.email, attachments={'available_files':f'{run_dir}/available_files.txt'})
                    os.system(f"ssh usfuser@{Config.SMS_SERVER} \"sendsms.py -m 'Dear_{researcher.username},_A_link_for_runID_{project_id}_was_send_to_{researcher.email}._{pw}_is_needed_to_unlock_the_link._Regards,_USEQ' -n {researcher.phone}\"")



            os.system(f'rm -r {upload_dir} {upload_dir_done}')
            session,Run, IlluminaSequencingStats,NanoporeSequencingStats = createDBSession()
            prev_results = session.query(NanoporeSequencingStats).filter_by(flowcell_id=run_info['flowcell_id']).first()
            if prev_results:
                sys.exit(f'Warning : Stats for {run_info["flowcell_id"]} where already uploaded to portal db. Skipping.')

            if run_info['stats_file']:
                tmp_dir = Path(f"{run_dir}/{run_info['flowcell_id']}")
                if not tmp_dir.is_dir():
                    tmp_dir.mkdir()
                os.system(f"cp {run_info['stats_file']} {tmp_dir}")

                rsync_command = f"/usr/bin/rsync -rah {tmp_dir} {Config.PORTAL_USER}@{Config.PORTAL_SERVER}:{Config.PORTAL_STORAGE}/"

                exit_code = os.system(rsync_command)
                if exit_code :
                    sys.exit(f'Error : Failed tot copy stats pdf to {Config.PORTAL_STORAGE}. Please fix this manually using link_run_results!')

                os.system(f"rm -r {tmp_dir}")

                nan_stats = NanoporeSequencingStats(
                    general_stats = Path(run_info['stats_file']).name,
                    date = run_info['date'],
                    date_started = run_info['date'],
                    date_send = datetime.datetime.today(),
                    flowcell_id = run_info['flowcell_id'],
                    run_id=portal_run.id
                )
                session.add(nan_stats)
                session.commit()
                print(f'Uploaded stats for {project_id} to portal db.')
            else:
                nan_stats = NanoporeSequencingStats(
                    general_stats = '',
                    date = run_info['date'],
                    date_started = run_info['date'],
                    date_send = datetime.datetime.today(),
                    flowcell_id = run_info['flowcell_id'],
                    run_id=portal_run.id
                )


                session.add(nan_stats)
                session.commit()
                print(f'Uploaded only flowcell ID for {project_id} to portal db. No stats pdf could be found.')
    else:

        run_dir, flowcell_id, run_meta = getIlluminaRunDetails(lims, project_name, fid)
        analysis_steps = samples[0].udf.get('Analysis','').split(',')
        if not run_dir:
            sys.exit(f'Error : No Illumina run directory could be found!')

        nextcloud_runid = f'{project_id}_{flowcell_id}'
        nextcloud_runida = f'{project_id}_A{flowcell_id}'
        nextcloud_runidb = f'{project_id}_B{flowcell_id}'
        if nextcloud_util.checkExists( nextcloud_runid ):
            nextcloud_runid = f'{project_id}_{flowcell_id}'
        elif nextcloud_util.checkExists( nextcloud_runida ):
            nextcloud_runid = nextcloud_runida
        elif nextcloud_util.checkExists( nextcloud_runidb ):
            nextcloud_runid = nextcloud_runidb
        else:
            sys.exit(f'Error : {project_id} was not uploaded to Nextcloud yet!')

        file_list = nextcloud_util.simpleFileList(nextcloud_runid)
        if not file_list:
            sys.exit(f"{name}\tError : No files found in nextcloud dir  {nextcloud_runid}!")

        available_files = open(f'{run_dir}/available_files.txt', 'w', newline='\n')
        for file in file_list:
            available_files.write(f"{file}\n")
        available_files.close()
        conversion_stats = parseConversionStats(lims, run_dir ,project_id )
        summary_stats = parseSummaryStats(run_dir)


        if not conversion_stats:
            print(f'Warning : No conversion stats could be found for {run_dir}!')

        if not summary_stats:
            sys.exit(f'Error : No summary stats could be found for {run_dir}!')

        print ("\nAre you sure you want to send the following datasets (yes/no): ")
        table = Texttable(max_width=0)
        table.add_rows([['Data','Project (ID:Name)','Client Email', '# Samples','Total Reads', '% Q30', 'Mean Quality']])
        if conversion_stats:
            table.add_row( [ run_dir.name, f"{project_id}:{project_name}", researcher.email, len(conversion_stats['samples']), conversion_stats['total_reads'],f"{conversion_stats['total_q30']}",conversion_stats['total_mean_qual']])
        else:
            table.add_row( [ run_dir.name, f"{project_id}:{project_name}", researcher.email, '?','?','?','?'])
        print (table.draw())

        if check():
            share_response = nextcloud_util.share(nextcloud_runid, researcher.email)

            if "ERROR" in share_response:
                sys.exit (f"{name}\tError : Failed to share {nextcloud_runid} with message:\n\t{share_response['ERROR']}")
            else:
                share_id = share_response["SUCCES"][0]
                pw = share_response["SUCCES"][1]

                template_data = {
                    'project_id' : project_id,
                    'phone' : researcher.phone,
                    'name' : f"{researcher.first_name} {researcher.last_name}",
                    'nextcloud_host' : Config.NEXTCLOUD_HOST,
                    'share_id' : share_id,
                    'file_list' : file_list,
                    'conversion_stats' : conversion_stats,
                    'sample_measurements' : sample_measurements,
                    'analysis_steps' : analysis_steps,
                }

                mail_content = renderTemplate('share_illumina_template.html', template_data)
                mail_subject = f"USEQ sequencing of sequencing-run ID {project_id} finished"
                #
                if Config.DEVMODE:
                    sendMail(mail_subject,mail_content, Config.MAIL_SENDER ,'s.w.boymans@umcutrecht.nl',attachments={'available_files':f'{run_dir}/available_files.txt'})
                    print (pw)
                else:
                    sendMail(mail_subject,mail_content, Config.MAIL_SENDER ,researcher.email,attachments={'available_files':f'{run_dir}/available_files.txt'})
                    os.system(f"ssh usfuser@{Config.SMS_SERVER} \"sendsms.py -m 'Dear_{researcher.username},_A_link_for_runID_{project_id}_was_send_to_{researcher.email}._{pw}_is_needed_to_unlock_the_link._Regards,_USEQ' -n {researcher.phone}\"")


                print(f'Shared {project_id} with {researcher.email}')



                session,Run, IlluminaSequencingStats,NanoporeSequencingStats = createDBSession()
                prev_results = session.query(IlluminaSequencingStats).filter_by(flowcell_id=flowcell_id, run_id=portal_run.id).first()
                if prev_results:
                    sys.exit(f'Warning : Stats for {flowcell_id} where already uploaded to portal db. Skipping.')

                if conversion_stats:
                    tmp_dir = Path(f"{run_dir}/{flowcell_id}")
                    if not tmp_dir.is_dir():
                        tmp_dir.mkdir()
                    conversion_stats_json_file = Path(f"{tmp_dir}/Conversion_Stats_{project_id}.json")
                    with open(conversion_stats_json_file, 'w') as c:
                        c.write( json.dumps( conversion_stats ) )

                    os.system(f"scp -r {run_dir}/Conversion/Reports/*png {tmp_dir}")

                    rsync_command = f"/usr/bin/rsync -rah {tmp_dir} {Config.PORTAL_USER}@{Config.PORTAL_SERVER}:{Config.PORTAL_STORAGE}/"
                    exit_code = os.system(rsync_command)
                    if exit_code :
                        sys.exit(f'Error : Failed tot copy plots to {Config.PORTAL_STORAGE}. Please fix this manually using link_run_results!')


                    ill_stats = IlluminaSequencingStats(
                        flowcell_id=flowcell_id,
                        general_stats=conversion_stats_json_file.name,
                        date=run_meta['date_started'],
                        date_started = run_meta['date_started'],
                        date_send = datetime.datetime.today(),
                        run_name = run_dir.name,
                        yield_r1 = summary_stats['yield_r1'],
                        yield_r2 = summary_stats['yield_r2'],
                        reads = summary_stats['reads'],
                        avg_quality_r1 = conversion_stats['avg_quality_r1'],
                        avg_quality_r2 = conversion_stats['avg_quality_r2'],
                        cluster_density = summary_stats['cluster_density'],
                        load_conc = run_meta['load_conc'],
                        perc_q30_r1 = summary_stats['perc_q30_r1'],
                        perc_q30_r2 = summary_stats['perc_q30_r2'],
                        perc_occupied = summary_stats['perc_occupied'],
                        phix_loaded = run_meta['phix_loaded'],
                        phix_aligned = summary_stats['phix_aligned'],
                        # succesful = #TODO
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
                    # print(ill_stats)
                    print(f'Uploaded stats for {project_id} to portal db.')
                else:
                    ill_stats = IlluminaSequencingStats(
                        flowcell_id=flowcell_id,
                        date=run_meta['date_started'],
                        date_started = run_meta['date_started'],
                        date_send = datetime.datetime.today(),
                        run_name = run_dir.name,
                        yield_r1 = summary_stats['yield_r1'],
                        yield_r2 = summary_stats['yield_r2'],
                        reads = summary_stats['reads'],
                        avg_quality_r1 = 0,
                        avg_quality_r2 = 0,
                        cluster_density = summary_stats['cluster_density'],
                        load_conc = run_meta['load_conc'],
                        perc_q30_r1 = summary_stats['perc_q30_r1'],
                        perc_q30_r2 = summary_stats['perc_q30_r2'],
                        perc_occupied = summary_stats['perc_occupied'],
                        phix_loaded = run_meta['phix_loaded'],
                        phix_aligned = summary_stats['phix_aligned'],
                        run_id=portal_run.id
                    )
                    print(f'Uploaded only flowcell_id for {project_id} to portal db.')
                    session.add(ill_stats)
                    # print(ill_stats)
                    session.commit()




def run(lims, ids, username, dir, fid, all_dirs_ont):
    """Runs raw, processed or manual function based on mode"""

    global nextcloud_util

    #Set up nextcloud
    nextcloud_util = NextcloudUtil()
    nextcloud_util.setHostname( Config.NEXTCLOUD_HOST )

    if ids:
        nextcloud_util.setup( Config.NEXTCLOUD_USER, Config.NEXTCLOUD_PW, Config.NEXTCLOUD_WEBDAV_ROOT,Config.NEXTCLOUD_RAW_DIR,Config.MAIL_SENDER )
        shareDataById(lims, ids, fid, all_dirs_ont)

    elif username and dir:
        nextcloud_util.setup( Config.NEXTCLOUD_USER, Config.NEXTCLOUD_PW, Config.NEXTCLOUD_WEBDAV_ROOT,Config.NEXTCLOUD_MANUAL_DIR,Config.MAIL_SENDER )
        shareDataByUser(lims, username, dir)
