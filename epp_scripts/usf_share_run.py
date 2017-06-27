import glsapiutil
import xml.dom.minidom
import smtplib
import codecs
import json
import urllib2
import os
import tarfile
import sys
import multiprocessing
from xml.dom.minidom import parseString
from optparse import OptionParser
from email.mime.text import MIMEText
from xml.parsers.expat import ExpatError
from nextcloud_util import NextcloudUtil
import datetime

HOSTNAME = ''
VERSION = ''
BASE_URI = ''

DEBUG = True
api = None
options = None
CACHE = {}
RUN_TYPES = ['NextSeq Run (NextSeq) 1.0', 'MiSeq Run (MiSeq) 4.0', 'HiSeq Run (HiSeq) 5.0']


def getObjectDOM( uri ):

    global CACHE
    #global api
    if uri not in CACHE.keys():
        #print uri
        thisXML = api.getResourceByURI( uri )
        # print thisXML
        thisDOM = parseString( thisXML )
        CACHE[ uri ] = thisDOM

    return CACHE[ uri ]


def setupGlobalsFromURI( uri ):

	global HOSTNAME
	global VERSION
	global BASE_URI

	tokens = uri.split( "/" )
	HOSTNAME = "/".join(tokens[0:3])
	VERSION = tokens[4]
	BASE_URI = "/".join(tokens[0:5]) + "/"

	if DEBUG is True:
		print HOSTNAME
		print BASE_URI

def getArtifacts( ids ):
	artifactLinksXML = []

	artifactLinksXML.append( '<ri:links xmlns:ri="http://genologics.com/ri">' )
	for limsid in ids:
		artifactLinksXML.append( '<link uri="' + BASE_URI + 'artifacts/' + limsid + '" rel="artifacts"/>' )

	artifactLinksXML.append( '</ri:links>' )
	artifactLinksXML = ''.join( artifactLinksXML )

	artifactsXML = api.getBatchResourceByURI( BASE_URI + "artifacts/batch/retrieve", artifactLinksXML )
	## did we get back anything useful?
	try:
		artifactsDOM = parseString( artifactsXML )
		nodes = artifactsDOM.getElementsByTagName( "art:artifact" )
		if len(nodes) > 0:
			response = nodes
		else:
			response = ""
	except:
		response = ""

	return response

def getSamples( ids ):

	sampleLinksXML = []
	sampleLinksXML.append( '<ri:links xmlns:ri="http://genologics.com/ri">' )

	for limsid in ids:
		sampleLinksXML.append( '<link uri="' + BASE_URI + 'samples/' + limsid + '" rel="sample"/>' )

	sampleLinksXML.append( '</ri:links>' )
	sampleLinksXML = ''.join( sampleLinksXML )
	samplesXML = api.getBatchResourceByURI( BASE_URI + "samples/batch/retrieve", sampleLinksXML )

	## did we get back anything useful?
	try:
		samplesDOM = parseString( samplesXML )
		nodes = samplesDOM.getElementsByTagName( "smp:sample" )
		if len(nodes) > 0:
			response = nodes
		else:
			response = ""
	except:
		response = ""

	return response

def getRunInfo( project_name ):

    processes_DOM = getObjectDOM("{0}processes/?projectname={1}&type={2}".format(BASE_URI, project_name, "&type=".join([urllib2.quote(x) for x in RUN_TYPES])))
    process_nodes = processes_DOM.getElementsByTagName("process")

    runs = {}
    for process in process_nodes:
        process.getAttribute("uri")
        process_DOM = getObjectDOM( process.getAttribute("uri") )
        run_date = process_DOM.getElementsByTagName("date-run")[0].firstChild.data
        run_id = api.getUDF(process_DOM, "Run ID")
        run_flowcell = api.getUDF(process_DOM, "Flow Cell ID")
        runs[run_date] = [run_id, run_flowcell]


    run_dates = [datetime.datetime.strptime(ts, "%Y-%m-%d") for ts in runs.keys()]
    run_dates.sort()
    sorted_run_dates = [datetime.datetime.strftime(ts, "%Y-%m-%d") for ts in run_dates]
    return runs[sorted_run_dates[-1]] #return the most recent run, this is the run we want to share

def getRunDirectory( run_name=None, run_flowcell=None ):
    run_dir = None
    if run_name:
        for item in os.listdir(options.dataDir):
        #print os.path.join( options.dataDir, item, run_name )
            if os.path.isdir( os.path.join( options.dataDir, item, run_name ) ):
                run_dir = os.path.join( options.dataDir, item, run_name )
    elif run_flowcell:
        res=[]
        for root,dirs,files in os.walk(options.dataDir, topdown=True):
            for d in dirs:
                path = os.path.join(root,d)
                if path.count("/") == 7:
                    if path.endswith(run_flowcell):
                        run_dir = path
                        break
    return run_dir

def zipRun( project_id, run_dir ):
    run_name = os.path.basename(run_dir)
    run_zip = "{0}/{1}.tar.gz".format(run_dir,project_id)

    # if not os.path.isfile(file_path): sys.exit("File path '{0}' is not a file".format(file_path))
    if os.path.isfile(run_zip):
        print "Zip file '{0}' already exists".format(run_zip)
    else:
        with tarfile.open(run_zip, "w:gz") as tar:
            tar.add(run_dir, arcname=run_name)
        tar.close()


    return run_zip

def getResearcherEmail( uri ):
    r_DOM = getObjectDOM( uri )
    email = r_DOM.getElementsByTagName( "email" )[0].firstChild.data
    if not email:
        sys.exit("Could not find email for researcher uri {}".format(uri))

    return email

def shareWorker(project_name, project_id, researcher_email):
    name = multiprocessing.current_process().name
    print "{0} : Starting".format(name)

    run_info = getRunInfo(project_name)
    run_name = run_info[0]
    run_flowcell = run_info[1]

    if run_name:
        run_dir = getRunDirectory( run_name = run_name )
    elif run_flowcell:
        run_dir = getRunDirectory( run_flowcell = run_flowcell )
    else:
        print "{0} : No run name found for {1}".format(name,project_name)
        return
    if not run_dir:
         print "{0} : Could not find {1} in {2}".format(name, run_dir, options.dataDir)
         return

    run_zip = zipRun( project_id,run_dir )

    upload_response = nc_util.upload(run_zip)
    if upload_response:
        print "{0} : Upload of {1} failed".format(name, run_zip)
        print "{0} : {1}".format(name, upload_response)
        return

    share_response = nc_util.share(run_zip, researcher_email)
    if share_response:
        print "{0} : Sharing of {1} failed".format(name, run_zip)
        print "{0} : {1}".format(name, share_response)
        return

    print "{0} : Finished".format(name)
    return



def shareFromProjectId():

    project_ids = options.projectids.split(",")
    share_workers = []
    for project_id in project_ids:

        p_XML = api.getResourceByURI( BASE_URI + 'projects/' + project_id)
        p_DOM = parseString( p_XML )

        project_name = p_DOM.getElementsByTagName( "name" )[0].firstChild.data
        researcher_uri = p_DOM.getElementsByTagName( "researcher" )[0].getAttribute( "uri" )

        researcher_email = getResearcherEmail( researcher_uri )

        run_info = getRunInfo(project_name)
        run_name = run_info[0]
        run_flowcell = run_info[1]

        share_worker = multiprocessing.Process(name="Worker_{0}".format(project_name), target=shareWorker, args=(project_name,project_id, researcher_email))

        share_workers.append( share_worker)
        share_worker.start()

    for w in share_workers:
        w.join()




def shareFromStep():


    stepURI = options.stepURI + "/details"
    stepDOM = getObjectDOM( stepURI )
    #Check if nextseq pool, else don't copy samplesheet

    sequencing_runs = {}
    #Get the input analytes lims ids
    analyteIDS = []
    for input in stepDOM.getElementsByTagName( "input" ):
        analyteID = input.getAttribute( "limsid" )


        if analyteID not in analyteIDS:
            analyteIDS.append( analyteID )

    #Get the input artifacts (which is a pool of samples) by lims id
    # print "AnalyteIDs","\t".join(analyteIDS)

    share_workers = []
    artifacts = getArtifacts( analyteIDS )
    for artifact in artifacts:
        pool_uri = artifact.getAttribute( "uri")
        pool_id = artifact.getAttribute( "limsid" )
        pool_name = artifact.getElementsByTagName("name")[0].firstChild.data
        first_sample_id = artifact.getElementsByTagName("sample")[0].getAttribute("limsid")
        first_sample_DOM = getSamples([first_sample_id])[0]
        project_id = first_sample_DOM.getElementsByTagName("project")[0].getAttribute("limsid")
        project_DOM = getObjectDOM( BASE_URI + 'projects/' + project_id )
        project_name = project_DOM.getElementsByTagName("name")[0].firstChild.data

        researcher_uri = project_DOM.getElementsByTagName( "researcher" )[0].getAttribute( "uri" )
        researcher_email = getResearcherEmail( researcher_uri )

        share_worker = multiprocessing.Process(name="Worker_{0}".format(project_name), target=shareWorker, args=(project_name,project_id, researcher_email))

        share_workers.append( share_worker)
        share_worker.start()

    for w in share_workers:
        w.join()

def main():

    global api
    global options
    global nc_util

    parser = OptionParser()
    parser.add_option( "-u", "--username", help = "username of the current user", action = 'store', dest = 'username' )
    parser.add_option( "-p", "--password", help = "password of the current user" )
    parser.add_option( "-s", "--stepURI", help = "the URI of the step that launched this script" )
    parser.add_option( "-i", "--projectids", help="The projectid(s) of the run you want to share. If multiple separate by comma." )
    parser.add_option( "-d", "--dataDir", help = "Root directory for sequencing runs ")


    nextcloud_hostname = "ncie01.op.umcutrecht.nl"
    api_uri = "https://usf-lims.umcutrecht.nl/api/v2/"
    #parser.add_option( "-o", "--outname", help = "Output file name + path" )

    (options, otherArgs) = parser.parse_args()

    if options.stepURI: setupGlobalsFromURI( options.stepURI )
    else: setupGlobalsFromURI( api_uri )

    api = glsapiutil.glsapiutil()
    api.setHostname( HOSTNAME )
    api.setVersion( VERSION )
    api.setup( options.username, options.password )

    nc_util = NextcloudUtil()
    nc_util.setHostname( nextcloud_hostname )
    nc_util.setup( options.username, options.password )


    if options.stepURI:
        shareFromStep()
    elif options.projectids:
        shareFromProjectId()




if __name__ == "__main__":

    main()
