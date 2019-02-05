import glsapiutil
from optparse import OptionParser
from xml.dom.minidom import parseString
import datetime
HOSTNAME = ''
VERSION = ''
BASE_URI = ''

DEBUG = True
api = None
options = None
CACHE = {}

def getObjectDOM( uri ):

	global CACHE
	#global api
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

def closeProjects(  ):

    stepURI = options.stepURI + "/details"
    stepDOM = getObjectDOM( stepURI )

    #Get the input analytes lims ids
    analyteIDS = []
    for input in stepDOM.getElementsByTagName( "input" ):
        analyteID = input.getAttribute( "limsid" )

        if analyteID not in analyteIDS:
            analyteIDS.append( analyteID )

    artifacts = getArtifacts( analyteIDS )
    #Get project ID
    for artifact in artifacts:
        first_sample_uri = artifact.getElementsByTagName("sample")[0].getAttribute("uri")
        first_sample = getObjectDOM( first_sample_uri )
        project_uri = first_sample.getElementsByTagName( "project" )[0].getAttribute( "uri" )
        project_id = first_sample.getElementsByTagName( "project" )[0].getAttribute( "limsid" )
        project = getObjectDOM( project_uri )
        if not project.getElementsByTagName("close-date"):
            try:
                close_date = project.createElement('close-date')

                current_date = datetime.datetime.today().strftime('%Y-%m-%d')
                close_date_text = project.createTextNode(current_date)
                close_date.appendChild(close_date_text)

                project.childNodes[0].appendChild(close_date)
                # print project.toxml()
                response = api.updateObject(project.toxml(), project_uri)
            except:
                print 'Failed to close project {0}'.format(project_id)


def main():

    global api
    global options

    parser = OptionParser()
    parser.add_option( "-u", "--username", help = "username of the current user", action = 'store', dest = 'username' )
    parser.add_option( "-p", "--password", help = "password of the current user" )
    parser.add_option( "-s", "--stepURI", help = "the URI of the step that launched this script" )

    (options, otherArgs) = parser.parse_args()

    setupGlobalsFromURI( options.stepURI )
    api = glsapiutil.glsapiutil()
    api.setHostname( HOSTNAME )
    api.setVersion( VERSION )
    api.setup( options.username, options.password )

    closeProjects()

if __name__ == "__main__":
	main()
