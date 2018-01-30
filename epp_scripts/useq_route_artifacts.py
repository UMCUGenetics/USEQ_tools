
import sys
from optparse import OptionParser
import logging
import glsapiutil
from xml.dom.minidom import parseString
from useq_route_artifacts_config import NEXT_STEPS

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

def routeAnalytes(  ):

    ## Step 1: Get the step XML
    process_URI = options.stepURI + "/details"
    process_DOM = getObjectDOM(process_URI)

    analytes = set()
    for io in process_DOM.getElementsByTagName( 'input-output-map' ):
        input_art = io.getElementsByTagName("input")[0].getAttribute("limsid")
        if options.input:
             analytes.add( input_art )
        else:
            output_art_type = io.getElementsByTagName("output")[0].getAttribute("type")
            if output_art_type == "Analyte":    # only analytes can be routed to different queues
                output_art = io.getElementsByTagName("output")[0].getAttribute("limsid")
                analytes.add( output_art )

    artifacts_to_route = {} # Set up the dictionary of destination stages


    artifacts_DOM = getArtifacts( analytes )

    for artifact in artifacts_DOM.getElementsByTagName( "art:artifact" ):
        artifact_URI = artifact.getAttribute("uri").split('?')[0]
        if options.next_protocol:
            next_step = NEXT_STEPS[ options.next_protocol ]
            if next_step not in artifacts_to_route:
                artifacts_to_route[ next_step ] = []
            artifacts_to_route[ next_step ].append(artifact_URI)
        else:
            samples = artifact.getElementsByTagName("sample")
            #If it's a pool there's going to be 1 or more samples, so get the first for info.
            #If it's not a pool there's going to be only 1 samples, so also get the first one for INFO
            sample_uri = samples[0].getAttribute("uri")
            sample = getObjectDOM(sample_uri)

            sample_udf_value = api.getUDF(sample, options.udf_name)

            next_step = NEXT_STEPS[sample_udf_value]
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
        rXML = rXML + '<assign stage-uri="' + stageURI + '">'
        for uri in a_ToGo:
            rXML = rXML + '<artifact uri="' + uri + '"/>'
        rXML = rXML + '</assign>'
        rXML = rXML + '</rt:routing>'
        response = api.POST( rXML, api.getBaseURI() + "route/artifacts/" )
        return response

    # Step 3: Send separate routing messages for each destination stage
    for stage, artifacts in artifacts_to_route.items():
        r = pack_and_send( stage, artifacts )
        if len( parseString( r ).getElementsByTagName( "rt:routing" ) ) > 0:
            msg = str( len(artifacts) ) + " samples were added to the " + stage + " step. "
        else:
            msg = r
        logging.debug( msg )

def main():

    global api
    global options

    parser = OptionParser()
    parser.add_option( "-u", "--username", help = "username of the current user", action = 'store', dest = 'username' )
    parser.add_option( "-p", "--password", help = "password of the current user" )
    parser.add_option( "-s", "--stepURI", help = "the URI of the step that launched this script" )
    parser.add_option("-n", "--next_protocol", help = "manually set next protocol by name")
    parser.add_option( "-f", "--udf_name", help = "set next protocol by udf name")
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
