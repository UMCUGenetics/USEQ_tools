import glsapiutil
import xml.dom.minidom
import smtplib
import codecs
import json
import urllib2
import os

from xml.dom.minidom import parseString
from optparse import OptionParser
from email.mime.text import MIMEText
from xml.parsers.expat import ExpatError
HOSTNAME = ''
VERSION = ''
BASE_URI = ''

DEBUG = False
api = None
options = None
CACHE = {}

def getObjectDOM( uri ):

	global CACHE

	if uri not in CACHE.keys():

		thisXML = api.getResourceByURI( uri )

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

def getRunName( pool_id ):
#print pool_id
	run_name = None

	try:
		run = getObjectDOM( BASE_URI + "processes/?inputartifactlimsid="+pool_id+"&type=" + urllib2.quote("NextSeq Run (NextSeq) 1.0" ))
		run_uri = run.getElementsByTagName("process")[0].getAttribute("uri")
		# print "RUN URI",run_uri
		run = getObjectDOM( run_uri )
		run_name = api.getUDF(run, "Run ID")
		# print run_name
	except:
		run_name = None

	return run_name

def getCurrentStageURI( artifact ):

	workflow_stages = artifact.getElementsByTagName( 'workflow-stage')
	current_stage = None
	for stage in workflow_stages:
		status = stage.getAttribute('status')
		if status == 'IN_PROGRESS':
			current_stage = stage.getAttribute('uri')
			break
	return current_stage

def getWorkflowStageURIs ( current_stage_uri ):
	uri_parts = current_stage_uri.split( "/" )
	#print uri_parts
	workflow_uri = BASE_URI + "/".join( uri_parts[5:-2] )
	#print workflow_uri

	workflow_stage_uris = []
	try:
		#print workflow_uri
		workflow = getObjectDOM( workflow_uri )

		workflow_stages = workflow.getElementsByTagName('stages')[0].getElementsByTagName('stage')
		for stage in workflow_stages:
			workflow_stage_uris.append(stage.getAttribute('uri'))
	except:
		workflow_stage_uris = []

	return workflow_stage_uris


def copySampleSheets():
	stepURI = options.stepURI + "/details"
	stepDOM = getObjectDOM( stepURI )

	#Check if nextseq pool, else don't copy samplesheet

	sequencing_runs = {}
	#Get the input / output mappings
	analyteIDS = {}
	for io_map in stepDOM.getElementsByTagName( "input-output-map" ):
		input_analyteID = io_map.getElementsByTagName("input")[0].getAttribute( "limsid" )
		output_analyteID = io_map.getElementsByTagName("output")[0].getAttribute( "limsid" )


		if input_analyteID not in analyteIDS:
			analyteIDS[ input_analyteID] = output_analyteID

	#Get the input artifacts (which is a pool of samples) by lims id
	# print "AnalyteIDs","\t".join(analyteIDS)
	artifacts = getArtifacts( analyteIDS.keys() )

	next_actions = {}
	for artifact in artifacts:
		pool_uri = artifact.getAttribute( "uri")
		#print artifact
		pool_id = artifact.getAttribute( "limsid" )
		#print pool_id
		#current_stage_uri = getCurrentStageURI( artifact )
		#workflow_stage_uris = getWorkflowStageURIs( current_stage_uri )
		next_action = 'repeat'
		next_actions[ pool_uri] = next_action
		#print current_stage_uri

		parent_process_uri = artifact.getElementsByTagName( "parent-process")[0].getAttribute("uri")
		#print parent_process_uri
		run_name = getRunName( pool_id )
		if not run_name:
			print "No run name found for pool: " + pool_id
			api.reportScriptStatus( options.stepURI, "WARNING", "No run name found for pool: " + pool_id )
			#add warning!!
			#next_actions[ pool_uri] = next_action
			continue

		run_dir = None
		for item in os.listdir(options.dataDir):
			#print os.path.join( options.dataDir, item, run_name )
			if os.path.isdir( os.path.join( options.dataDir, item, run_name ) ):
				run_dir = os.path.join( options.dataDir, item, run_name )

		if not run_dir:
			print "Run directory "+run_dir+" does not exist"
			api.reportScriptStatus( options.stepURI, "WARNING", "Run directory : " + run_dir + "for pool " +pool_id+ " does not exist" )
			#add warning!!
			#next_actions[ pool_uri] = next_action
			continue

		print "Run Dir " + run_dir + "\n"
		output_artifact_ids = []
		parent_process = getObjectDOM( parent_process_uri )
		for output_artifact in parent_process.getElementsByTagName( "output" ):
			output_artifact_ids.append(output_artifact.getAttribute( "limsid" ))
			# print output_artifact.getAttribute( "limsid" )

		output_artifacts = getArtifacts( output_artifact_ids )
		samplesheet = ''
		for output_artifact in output_artifacts:
			name = output_artifact.getElementsByTagName("name")[0].firstChild.data
			# print 'artifact name',name

			if name == 'SampleSheet csv':

				samplesheet_uri = output_artifact.getElementsByTagName("file:file")[0].getAttribute("uri")
				download_response = urllib2.urlopen( samplesheet_uri + '/download')

				samplesheet = download_response.read()

				samplesheet_file = codecs.open(run_dir + '/SampleSheet-test.csv', 'w', 'utf-8')
				samplesheet_file.write( samplesheet )
				samplesheet_file.close()




def main():

	global api
	global options

	parser = OptionParser()
	parser.add_option( "-u", "--username", help = "username of the current user", action = 'store', dest = 'username' )
	parser.add_option( "-p", "--password", help = "password of the current user" )
	parser.add_option( "-s", "--stepURI", help = "the URI of the step that launched this script" )
	parser.add_option( "-d", "--dataDir", help = "Root directory for sequencing runs ")
	#parser.add_option( "-o", "--outname", help = "Output file name + path" )

	(options, otherArgs) = parser.parse_args()

	setupGlobalsFromURI( options.stepURI )
	api = glsapiutil.glsapiutil()
	api.setHostname( HOSTNAME )
	api.setVersion( VERSION )
	api.setup( options.username, options.password )

	copySampleSheets()




if __name__ == "__main__":
	main()
