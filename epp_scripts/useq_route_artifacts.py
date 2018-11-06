import re
import sys
from optparse import OptionParser
import logging
import glsapiutil
from xml.dom.minidom import parseString
from useq_route_artifacts_config import NEXT_STEPS, STEP_NAMES

DEBUG = True
api = None
options = None
CACHE = {}

def getObjectDOM( uri ):

    global CACHE
    #global api
    if uri not in CACHE.keys():
        #print uri
        this_XML = api.getResourceByURI( uri )
        # print this_XML
        this_DOM = parseString( this_XML )
        CACHE[ uri ] = this_DOM

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
    # print artifactsXML
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

def routeAnalytes(  ):

    ## Step 1: Get the step XML
    process_URI = options.stepURI + "/details"
    process_DOM = getObjectDOM(process_URI)
    current_step = process_DOM.getElementsByTagName("configuration")[0].firstChild.data

    analytes = set()
    for io in process_DOM.getElementsByTagName( 'input-output-map' ):
        input_art = io.getElementsByTagName("input")[0].getAttribute("limsid")
        if options.input:
            # print 'using input'
            analytes.add( input_art )
        else:
            # print 'using output'
            output_art_type = io.getElementsByTagName("output")[0].getAttribute("type")
            if output_art_type == "Analyte":    # only analytes can be routed to different queues
                output_art = io.getElementsByTagName("output")[0].getAttribute("limsid")
                print output_art
                analytes.add( output_art )

    artifacts_to_route = {} # Set up the dictionary of destination stages


    artifacts = getArtifacts( analytes )

    for artifact in artifacts:
        artifact_URI = artifact.getAttribute("uri").split('?')[0]
        samples = artifact.getElementsByTagName("sample")
        sample_uri = samples[0].getAttribute("uri")
        # sample_id =
        sample = getObjectDOM(sample_uri)
        project_uri = sample.getElementsByTagName('project')[0].getAttribute('uri')
        project = getObjectDOM(project_uri)
        project_application = api.getUDF(project, 'Application')
        next_step = None

        if current_step in STEP_NAMES['ISOLATION']:
            if project_application == 'USF - SNP genotyping':
                next_step = NEXT_STEPS['USEQ - Quant Studio Fingerprinting']
            else:
                sample_libprep = api.getUDF(sample, 'Library prep kit')
                next_step = NEXT_STEPS[sample_libprep]

        elif current_step in STEP_NAMES['LIBPREP']:
            next_step = NEXT_STEPS['USEQ - Library Pooling']

        elif current_step in STEP_NAMES['POOLING']:
            sample_type = api.getUDF(sample, 'Sample Type')
            if sample_type == 'DNA library' or sample_type == 'RNA library': #Go to pool QC
                next_step = NEXT_STEPS['USEQ - Pool QC']
            else: #Pool QC has already been done
                sample_platform = api.getUDF(sample, 'Platform')
                next_step = NEXT_STEPS[sample_platform]

        elif current_step in STEP_NAMES['POOL QC']:
            sample_platform = api.getUDF(sample, 'Platform')
            next_step = NEXT_STEPS[sample_platform]

        elif current_step in STEP_NAMES['SEQUENCING']:

            next_step = NEXT_STEPS['USEQ - Post Sequencing']

        elif current_step in STEP_NAMES['POST SEQUENCING']:
            # print 'post sequencing'
            sample_analyses = api.getUDF(sample,'Analysis').split(",")

            if len(sample_analyses) == 1 and 'Raw data (FastQ)' in sample_analyses:
                next_step = NEXT_STEPS['USEQ - Encrypt & Send']
            else:
                next_step = NEXT_STEPS['USEQ - Analysis']
                # print sample_analyses
        if next_step not in artifacts_to_route:
            artifacts_to_route[ next_step ] = []
        artifacts_to_route[ next_step ].append(artifact_URI)


    if len( artifacts_to_route ) == 0:
        msg = "INFO: No derived samples were routed."
        logging.debug( msg )
        sys.exit(0)

    def pack_and_send( stageURI, a_ToGo ):
        ## Build and POST the routing message
        rXML = '<rt:routing xmlns:rt="http://genologics.com/ri/routing">'
        workflowURI = "/".join(stageURI.split("/")[0:-2])

        rXML = rXML + '<assign stage-uri="' + stageURI + '">'
        for uri in a_ToGo:
            rXML = rXML + '<artifact uri="' + uri + '"/>'
        rXML = rXML + '</assign>'

        rXML = rXML + '</rt:routing>'

        response = api.createObject( rXML, BASE_URI + "route/artifacts/")
        return response
    msg = ''
    # Step 3: Send separate routing messages for each destination stage
    for stage, artifacts in artifacts_to_route.items():
        r = pack_and_send( stage, artifacts )
        # print r
        if len( parseString( r ).getElementsByTagName( "rt:routing" ) ) > 0:
            msg = str( len(artifacts) ) + " samples were added to the " + stage + " step. "
        else:
            msg = r
        logging.debug( msg )
    # print msg
    return msg
def main():

    global api
    global options

    parser = OptionParser()
    parser.add_option( "-u", "--username", help = "username of the current user", action = 'store', dest = 'username' )
    parser.add_option( "-p", "--password", help = "password of the current user" )
    parser.add_option( "-s", "--stepURI", help = "the URI of the step that launched this script" )

    parser.add_option( "-i", "--input", action="store_true", default=False, help="uses input artifact UDFs")             # input or output artifact - Default is output
    (options, otherArgs) = parser.parse_args()

    setupGlobalsFromURI( options.stepURI )
    api = glsapiutil.glsapiutil()
    api.setHostname( HOSTNAME )
    api.setVersion( VERSION )
    api.setup( options.username, options.password )

    ## at this point, we have the parameters the EPP plugin passed, and we have network plumbing
    ## so let's get this show on the road!
    routeAnalytes()

# mode = getStepWorkflow( options.stepURI + "/details" )
if __name__ == "__main__":
    main()
