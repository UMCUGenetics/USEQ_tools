from genologics.entities import Project
from config import RUN_PROCESSES,RUN_DIR,NEXTCLOUD_HOST,NEXTCLOUD_WEBDAV_ROOT,NEXTCLOUD_RUN_DIR,MAIL_SENDER, NEXTCLOUD_USER, NEXTCLOUD_PW
from os.path import expanduser
from texttable import Texttable
import datetime
import gnupg
import os
import multiprocessing
import subprocess
from modules.useq_illumina_parsers import parseConversionStats, parseRunParameters
from modules.useq_nextcloud import NextcloudUtil
from modules.useq_mail import sendMail
from modules.useq_template import TEMPLATE_PATH,TEMPLATE_ENVIRONMENT,renderTemplate
import sys
import tarfile
GPG_DIR = expanduser("~/.gnupg/")



def zipRun( project_id, run_dir):
    run_name = os.path.basename(run_dir)
    run_zip = "{0}/{1}.tar.gz".format(run_dir,project_id)

    with tarfile.open(run_zip, "w:gz") as tar:
        tar.add(run_dir, arcname=run_name)

    return run_zip

def encryptRun( run_zip ,client_mail):
    run_encrypted = "{0}.gpg".format(run_zip)
    if os.path.isfile(run_encrypted):
        os.remove(run_encrypted)

    #Wanted to use gnupg module for this, but it doesn't support encrypting 'large' files
    try:
        subprocess.check_output("gpg --encrypt --output {0} --recipient '{1}' {2}".format(run_encrypted,client_mail, run_zip), shell=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        return e.output

    return run_encrypted

def shareProcess(project_name, project_id, run_dir,client_mail):

    name = multiprocessing.current_process().name
    print "{0}\tStarting".format(name)

    conversion_stats = parseConversionStats( "{0}/Data/Intensities/BaseCalls/Stats/ConversionStats.xml".format(run_dir) )
    if not conversion_stats:
        print "{0}\tError : No ConversionStats.xml file could be found in {1}/Data/Intensities/BaseCalls/Stats/!".format(name,run_dir)
        return

    expected_yield = parseRunParameters( "{0}/RunParameters.xml".format(run_dir) )
    if not expected_yield:
        print "{0}\tError : No RunParameters.xml file could be found in {1}!".format(name,run_dir)
        return

    print "{0}\tRunning compression".format(name)
    run_zip = zipRun( project_id, run_dir )
    # run_zip = "{0}/{1}.tar.gz".format(run_dir,project_id)
    if not os.path.isfile(run_zip):
        print "{0}\tError : {1}/{2}.tar.gz was not properly created!".format(name,run_dir,project_id)
        return

    print "{0}\tRunning encryption".format(name)
    run_encrypted = encryptRun(run_zip, client_mail)
    # run_encrypted = "{0}/{1}.tar.gz.gpg".format(run_dir,project_id)
    if not os.path.isfile(run_encrypted):
        print "{0}\tError : Something went wrong during encryption of {1}/{2}.tar.gz with error message:\n\t{3}".format(name,run_dir,project_id, run_encrypted)
        return

    print "{0}\tRunning upload to NextCloud".format(name)
    upload_response = nextcloud_util.upload(run_encrypted)
    if "ERROR" in upload_response:
        print "{0}\tError : Failed to upload {1} with message:\n\t{2}".format(name, run_encrypted, upload_response["ERROR"])
        return

    print "{0}\tSharing run {1}({2}) with {3}".format(name, project_id, run_dir, client_mail)
    share_response = nextcloud_util.share(run_encrypted, client_mail)
    if "ERROR" in share_response:
        print "{0}\tError : Failed to share {1} with message:\n\t{2}".format(name, run_encrypted, share_response["ERROR"])
        return
    else:
        share_id = share_response["SUCCES"]
        template_data = {
            'project_id' : project_id,
            'nextcloud_host' : NEXTCLOUD_HOST,
            'share_id' : share_id,
            'expected_reads' : expected_yield,
            'raw_reads' : conversion_stats['total_reads_raw'],
            'filtered_reads' : conversion_stats['total_reads'],
            'conversion_stats' : conversion_stats
        }

        mail_content = renderTemplate('run_share_template.html', template_data)
        mail_subject = "USEQ sequencing of sequencing-run ID {0} finished".format(project_id)
        sendMail(mail_subject,mail_content, MAIL_SENDER ,client_mail)

        os.remove(run_zip)
        os.remove(run_encrypted)

    return

def check( run_info):

    print "\nAre you sure you want to send the following run(s) (yes/no): "
    table = Texttable(max_width=0)
    table.add_rows([['Project ID','Project Name','Run Dir','Client Email']])
    for project in run_info:
        table.add_row( [ project['project_id'],project['project_name'],project['run_dir'],project['researcher_email'] ])
    print table.draw()

    yes = set(['yes','y', 'ye', ''])
    no = set(['no','n'])
    choice = raw_input().lower()
    if choice in yes:
       choice = True
    elif choice in no:
       choice = False
    else:
       sys.stdout.write("Please respond with 'yes' or 'no'")
    return choice


def getRunInfo( lims, project_name ):
    """Get the most recent run info based on project name and allowed RUN_PROCESSES"""
    runs = {}

    project_processes = lims.get_processes(
        projectname=project_name,
        type=RUN_PROCESSES
    )

    for process in project_processes:
        run_id = None
        flowcell_id = None
        if 'Run ID' in process.udf: run_id = process.udf['Run ID']
        if 'Flow Cell ID' in process.udf: flowcell_id = process.udf['Flow Cell ID']
        runs[ process.date_run ] = [  run_id, flowcell_id ]

    if not runs:
        return None

    run_dates = [datetime.datetime.strptime(ts, "%Y-%m-%d") for ts in runs.keys()]
    sorted_run_dates = [datetime.datetime.strftime(ts, "%Y-%m-%d") for ts in sorted(run_dates)]
    recent_run = runs[sorted_run_dates[-1]] #the most recent run, this is the run we want to share

    #Try to determine run directory
    if recent_run[0]: #run name is known
        for path in os.listdir(RUN_DIR):
            if os.path.isdir( os.path.join( RUN_DIR, path, recent_run[0])):
                recent_run.append( os.path.join( RUN_DIR, path, recent_run[0]) )
                break
    elif recent_run[1]: #run flowcell is known
        for root,dirs,files in os.walk(RUN_DIR, topdown=True):
            for dir in dirs:
                path = os.path.join(root,dir)
                if path.endswith("_000000000-"+recent_run[1]): #MiSeq
                    recent_run.append( path )
                    break
                elif path.endswith("_"+recent_run[1]): #NextSeq
                    recent_run.append( path )
                    break
                elif path.endswith("A"+recent_run[1]) or path.endswith("B"+recent_run[1]): #HiSeq
                    recent_run.append( path )
                    break

    return recent_run

def sendRuns(lims, ids):
    """Get's the run names, encrypts the run data and sends it to the appropriate client"""
    project_ids = ids.split(",")
    run_info = []

    for project_id in project_ids:
        project = None
        project_name = ''
        try:
            project = Project(lims, id=project_id)
            project_name = project.name
        except:
            print "Error : Project ID {0} not found!".format(project_id)
            continue

        researcher = project.researcher
        #Check if client has a gpg-key
        if not researcher.email.lower() in gpg_key_list:
            print "Error : User ID {0} ({1}) for project ID {2} has not provided a public key yet!".format(researcher.username,researcher.email, project_id)
            continue

        #Get run info
        info = getRunInfo(lims, project_name)
        if not info:
            print "Error : No run dir could be found for project ID {0}!".format(project_id)
            continue

        #Got all the info we need
        run_info.append({
            'project_id' : project_id,
            'project_name' : project_name,
            'run_name' : info[0],
            'run_flowcell' : info[1],
            'run_dir' : info[2],
            'researcher_email' : researcher.email

        })
    if not run_info:
        print "Error : None of the provided project IDs are able to be processed!"
    elif check(run_info):
        #Start sharing threads
        share_processes = []
        for project in run_info:
            share_process = multiprocessing.Process(name="Process_{0}".format(project['project_id']), target=shareProcess, args=(project['project_name'], project['project_id'], project['run_dir'],project['researcher_email']) )
            share_processes.append(share_process)
            share_process.start()

        for process in share_processes:
            process.join()


def run(lims, ids):
    """Runs sendRuns function and not much else"""
    global gpg
    global gpg_key_list
    global nextcloud_util

    #Set up gpg keychain
    gpg = gnupg.GPG(homedir=GPG_DIR)
    gpg_key_list = {}
    for key in gpg.list_keys():
        gpg_key_list [ key['uids'][0].split("<")[1][:-1].lower() ] = key

    #Set up nextcloud
    nextcloud_util = NextcloudUtil()
    nextcloud_util.setHostname( NEXTCLOUD_HOST )
    nextcloud_util.setup( NEXTCLOUD_USER, NEXTCLOUD_PW, NEXTCLOUD_WEBDAV_ROOT,NEXTCLOUD_RUN_DIR,MAIL_SENDER )

    sendRuns(lims,ids)
