from genologics.entities import Project
from config import RUN_PROCESSES, RAW_DIR, PROCESSED_DIR, NEXTCLOUD_HOST,NEXTCLOUD_WEBDAV_ROOT,NEXTCLOUD_RAW_DIR,NEXTCLOUD_PROCESSED_DIR,MAIL_SENDER, NEXTCLOUD_USER, NEXTCLOUD_PW
from os.path import expanduser, exists
from texttable import Texttable
import datetime
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



def zipRun( dir, dir_info):
    run_name = os.path.basename(dir)
    zip_name = "-".join(dir_info['projects'].keys())
    run_zip = "{0}/{1}.tar.gz".format(dir,zip_name)

    with tarfile.open(run_zip, "w:gz") as tar:
        tar.add(dir, arcname=run_name)

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


def shareProcessed(dir,dir_info):
    name = multiprocessing.current_process().name
    print "{0}\tStarting".format(name)

    print "{0}\tRunning compression".format(name)
    run_zip = zipRun( dir, dir_info )
    if not os.path.isfile(run_zip):
        print "{0}\tError : {1}/{2}.tar.gz was not properly created!".format(name,dir,dir_info['projects'].keys()[0])
        return

    print "{0}\tRunning encryption".format(name)
    run_encrypted = encryptRun(run_zip, dir_info['researcher_email'])

    if not os.path.isfile(run_encrypted):
        print "{0}\tError : Something went wrong during encryption of {1}/{2}.tar.gz with error message:\n\t{3}".format(name,dir,dir_info['projects'].keys()[0], run_encrypted)
        return

    print "{0}\tRunning upload to NextCloud".format(name)
    upload_response = nextcloud_util.upload(run_encrypted)
    if "ERROR" in upload_response:
        print "{0}\tError : Failed to upload {1} with message:\n\t{2}".format(name, run_encrypted, upload_response["ERROR"])
        return

    print "{0}\tSharing run {1} with {2}".format(name, dir, dir_info['researcher_email'])
    share_response = nextcloud_util.share(run_encrypted, dir_info['researcher_email'])
    if "ERROR" in share_response:
        print "{0}\tError : Failed to share {1} with message:\n\t{2}".format(name, run_encrypted, share_response["ERROR"])
        return
    else:
        share_id = share_response["SUCCES"]
        template_data = {
            'project_ids' : ",".join(dir_info['projects'].keys()),
            'nextcloud_host' : NEXTCLOUD_HOST,
            'share_id' : share_id

        }

        mail_content = renderTemplate('share_processed_template.html', template_data)
        mail_subject = "UBEC analysis of sequencing-run ID(s) {0} finished".format(",".join(dir_info['projects'].keys()))

        sendMail(mail_subject,mail_content, MAIL_SENDER ,dir_info['researcher_email'])
        os.remove(run_zip)
        os.remove(run_encrypted)

    return

def shareRaw(dir,dir_info):

    name = multiprocessing.current_process().name
    print "{0}\tStarting".format(name)

    conversion_stats = parseConversionStats( "{0}/Data/Intensities/BaseCalls/Stats/ConversionStats.xml".format(dir) )
    if not conversion_stats:
        print "{0}\tError : No ConversionStats.xml file could be found in {1}/Data/Intensities/BaseCalls/Stats/!".format(name,dir)
        return

    expected_yield = parseRunParameters( "{0}/RunParameters.xml".format(dir) )
    if not expected_yield:
        print "{0}\tError : No RunParameters.xml file could be found in {1}!".format(name,dir)
        return

    print "{0}\tRunning compression".format(name)
    run_zip = zipRun( dir, dir_info )
    if not os.path.isfile(run_zip):
        print "{0}\tError : {1}/{2}.tar.gz was not properly created!".format(name,dir,dir_info['projects'].keys()[0])
        return

    print "{0}\tRunning encryption".format(name)
    run_encrypted = encryptRun(run_zip, dir_info['researcher_email'])

    if not os.path.isfile(run_encrypted):
        print "{0}\tError : Something went wrong during encryption of {1}/{2}.tar.gz with error message:\n\t{3}".format(name,dir,dir_info['projects'].keys()[0], run_encrypted)
        return

    print "{0}\tRunning upload to NextCloud".format(name)
    upload_response = nextcloud_util.upload(run_encrypted)
    if "ERROR" in upload_response:
        print "{0}\tError : Failed to upload {1} with message:\n\t{2}".format(name, run_encrypted, upload_response["ERROR"])
        return

    print "{0}\tSharing run {1} with {2}".format(name, dir, dir_info['researcher_email'])
    share_response = nextcloud_util.share(run_encrypted, dir_info['researcher_email'])
    if "ERROR" in share_response:
        print "{0}\tError : Failed to share {1} with message:\n\t{2}".format(name, run_encrypted, share_response["ERROR"])
        return
    else:
        share_id = share_response["SUCCES"]
        template_data = {
            'project_id' : dir_info['projects'].keys()[0],
            'nextcloud_host' : NEXTCLOUD_HOST,
            'share_id' : share_id,
            'expected_reads' : expected_yield,
            'raw_reads' : conversion_stats['total_reads_raw'],
            'filtered_reads' : conversion_stats['total_reads'],
            'conversion_stats' : conversion_stats
        }

        mail_content = renderTemplate('share_raw_template.html', template_data)
        mail_subject = "USEQ sequencing of sequencing-run ID {0} finished".format(dir_info['projects'].keys()[0])
        sendMail(mail_subject,mail_content, MAIL_SENDER ,dir_info['researcher_email'])

        os.remove(run_zip)
        os.remove(run_encrypted)

    return

def check( run_info ):

    print "\nAre you sure you want to send the following datasets(s) (yes/no): "
    table = Texttable(max_width=0)

    table.add_rows([['Dir','Project(s) (ID:Name)','Client Email']])
    for datadir in run_info:
        projects = ",".join( ["{0}:{1}".format(id,name) for id,name in run_info[datadir]['projects'].iteritems() ] )
        table.add_row( [ datadir, projects, run_info[datadir]['researcher_email'] ])
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

def getProcessedData( lims, project_name, project_id ):
    """Get the most recent processed run info based on project name and allowed RUN_PROCESSES"""
    runs = []

    project_processes = lims.get_processes(
        projectname=project_name,
        type=RUN_PROCESSES
    )

    for process in project_processes:
        run_id = None
        flowcell_id = None
        if 'Run ID' in process.udf:
            run_id = process.udf['Run ID']
            for path in os.listdir(PROCESSED_DIR):
                if os.path.isdir( os.path.join( PROCESSED_DIR, path, run_id)):
                    return os.path.join( PROCESSED_DIR, path, run_id)


        if 'Flow Cell ID' in process.udf:
            flowcell_id = process.udf['Flow Cell ID']
            for root,dirs,files in os.walk(PROCESSED_DIR, topdown=True):
                for dir in dirs:
                    path = os.path.join(root,dir)
                    if path.endswith("_000000000-"+flowcell_id): #MiSeq
                        return path

                    elif path.endswith("_"+flowcell_id): #NextSeq
                        return path

                    elif path.endswith("A"+flowcell_id) or path.endswith("B"+flowcell_id): #HiSeq
                        return path

        for root,dirs,files in os.walk(PROCESSED_DIR, topdown=True):
            for dir in dirs:
                path = os.path.join(root,dir)
                if project_id in path:
                    return path

        return


def getRawData( lims, project_name ):
    """Get the most recent raw run info based on project name and allowed RUN_PROCESSES"""
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
        for path in os.listdir(RAW_DIR):
            if os.path.isdir( os.path.join( RAW_DIR, path, recent_run[0])):
                return os.path.join( RAW_DIR, path, recent_run[0])

    elif recent_run[1]: #run flowcell is known
        for root,dirs,files in os.walk(RAW_DIR, topdown=True):
            for dir in dirs:
                path = os.path.join(root,dir)
                if path.endswith("_000000000-"+recent_run[1]): #MiSeq
                    return path
                elif path.endswith("_"+recent_run[1]): #NextSeq
                    return path
                elif path.endswith("A"+recent_run[1]) or path.endswith("B"+recent_run[1]): #HiSeq
                    return path


def shareData(lims, mode,ids):
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
            print "Error : Project ID {0} not found!".format(project_id)
            continue

        researcher = project.researcher
        #Check if client has a gpg-key
        if not researcher.email.lower() in gpg_key_list:
            print "Error : User ID {0} ({1}) for project ID {2} has not provided a public key yet!".format(researcher.username,researcher.email, project_id)
            continue

        #Get run info
        info = None
        if mode == 'raw' :
            datadir = getRawData(lims, project_name)
        else :
            datadir = getProcessedData(lims, project_name, project_id)

        if not datadir:
            print "Error : No dir could be found for project ID {0}!".format(project_id)
            continue

        #Got all the info we need
        if datadir not in run_info:
            run_info[datadir] = {
                'researcher_email' : researcher.email,
                'projects' : {}
            }

        run_info[datadir]['projects'][project_id] = project_name

    if not run_info:
        print "Error : None of the provided project IDs are able to be processed!"
    elif check(run_info):
        #Start sharing threads
        share_processes =[]
        for datadir in run_info:
            share_process = None
            if mode == 'raw':
                share_process = multiprocessing.Process(name="Process_{0}".format(os.path.basename(datadir)), target=shareRaw, args=(datadir, run_info[datadir]) )
            else:
                share_process = multiprocessing.Process(name="Process_{0}".format(os.path.basename(datadir)), target=shareProcessed, args=(datadir, run_info[datadir]) )
            share_processes.append(share_process)
            share_process.start()

        for process in share_processes:
            process.join()

def run(lims, mode, ids):
    """Runs raw or processed function based on mode"""
    import gnupg
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
    if mode == 'raw':
        nextcloud_util.setup( NEXTCLOUD_USER, NEXTCLOUD_PW, NEXTCLOUD_WEBDAV_ROOT,NEXTCLOUD_RAW_DIR,MAIL_SENDER )
    else:
        nextcloud_util.setup( NEXTCLOUD_USER, NEXTCLOUD_PW, NEXTCLOUD_WEBDAV_ROOT,NEXTCLOUD_PROCESSED_DIR,MAIL_SENDER )
    shareData(lims, mode, ids)
