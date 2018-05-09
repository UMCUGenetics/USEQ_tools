import glsapiutil
import xml.dom.minidom
import smtplib

from xml.dom.minidom import parseString
from optparse import OptionParser
from email.mime.text import MIMEText
#from email.mime.multipart import MIMEMultipart

HOSTNAME = ''
VERSION = ''
BASE_URI = ''

DEBUG = False
api = None
options = None
CACHE = {}

RECIPIENTS = [ "s.w.boymans@umcutrecht.nl", "R.R.E.Janssen-10@umcutrecht.nl" ]

def getObjectDOM( uri ):

	global CACHE

	if uri not in CACHE.keys():
		thisXML = api.getResourceByURI( uri )
		thisDOM = parseString( thisXML )
		CACHE[ uri ] = thisDOM

	return CACHE[ uri ]

def sendMessage( msgSubject, msgText ):

	msg = MIMEText( msgText.encode('utf-8') , 'html')

	me = "useq@umcutrecht.nl"

	msg[ "Subject" ] = msgSubject
	msg[ "From" ] = me
	msg[ "To" ] = ";".join( RECIPIENTS )

	s = smtplib.SMTP( "localhost" )
	s.sendmail( me, RECIPIENTS, msg.as_string() )
	s.quit()

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

def getSamples( LUIDs ):

	"""
	This function will be passed a list of sample LUIDS, and return those sample represented as XML
	The samples will be collected in a single batch transaction, and the function will return the XML
	for the entire transactional list
	"""

	lXML = []
	lXML.append( '<ri:links xmlns:ri="http://genologics.com/ri">' )
	for limsid in LUIDs:
		lXML.append( '<link uri="' + BASE_URI + 'samples/' + limsid + '" rel="sample"/>' )
	lXML.append( '</ri:links>' )
	lXML = ''.join( lXML )

	mXML = api.getBatchResourceByURI( BASE_URI + "samples/batch/retrieve", lXML )

	## did we get back anything useful?
	try:
		mDOM = parseString( mXML )
		nodes = mDOM.getElementsByTagName( "smp:sample" )
		if len(nodes) > 0:
			response = mXML
		else:
			response = ""
	except:
		response = ""

	return response

def buildMessage():

	TEXT = []
	TABLE = []
	## get the xml for the corresponding step / process
	stepURI = options.stepURI + "/details"
	stepXML = api.getResourceByURI( stepURI )
	stepDOM = parseString( stepXML )	

	## get the container name from the process UDF (Flow Cell ID)
	cName = api.getUDF( stepDOM, "Flow Cell ID" )

	## get the input analytes
	iLUIDS = []
	for input in stepDOM.getElementsByTagName( "input" ):
		iLUID = input.getAttribute( "limsid" )
		if iLUID not in iLUIDS:
			iLUIDS.append( iLUID ) 

	sLUIDS = []
	## since the number of inputs will always be small, don't worry about batch
	for iLUID in iLUIDS:
		iURI = BASE_URI + "artifacts/" + iLUID
		iXML = api.getResourceByURI( iURI )
		iDOM = parseString( iXML )

		## get the corresponding samples
		for sample in iDOM.getElementsByTagName( "sample" ):
			sLUID = sample.getAttribute( "limsid" )
			if sLUID not in sLUIDS:
				sLUIDS.append( sLUID )

	## get the samples by a batch transaction
	bsXML = getSamples( sLUIDS )
	#print bsXML
	bsDOM = parseString( bsXML )
	pNames = []
	pIDs = []
	rUserName = ""
	rEmail = ""

	TABLE.append( "<table> <thead > <tr> <th><b>Sample</b></th> <th><b>Project</b></th> <th><b>Project ID</b></th> <th><b>Analysis</b></th> <th><b>Reference Genome</b></th></tr> </thead>" ) 
	TABLE.append( "<tbody>" )
	for sample in bsDOM.getElementsByTagName( "smp:sample" ):

		sLUID = sample.getAttribute( "limsid" )
		## get the UDFs we want
		refGenome = api.getUDF( sample, "Reference Genome" )
		analysis = api.getUDF( sample, "Analysis" )
		sName = sample.getElementsByTagName( "name" )[0].firstChild.data
		pName = ""
		pLUID = ""
		## get the corresponding project??
		try:
			pLUID = sample.getElementsByTagName( "project" )[0].getAttribute( "limsid" )
		except:
			pLUID = ""

		if len( pLUID ) == 0:
			pName = ""
		else:
			pURI = BASE_URI + "projects/" + pLUID
			pDOM = getObjectDOM( pURI )
			pName = pDOM.getElementsByTagName( "name" )[0].firstChild.data
			
			rURI = pDOM.getElementsByTagName( "researcher" )[0].getAttribute( "uri" )
			rURI = rURI.replace( ":8443" , "" )
			#rURI = rURI.replace( "http://" , "")
			rDOM = getObjectDOM( rURI )
			rFirstName = rDOM.getElementsByTagName( "first-name" )[0].firstChild.data
			rLastName = rDOM.getElementsByTagName( "last-name" )[0].firstChild.data
			rEmail = rDOM.getElementsByTagName( "email" )[0].firstChild.data
		
		if pName not in pNames:
			pNames.append(pName)		

		if pLUID not in pIDs:
			pIDs.append(pLUID)
		## build a hyperlink for search purposes:
		searchURI = HOSTNAME + "/clarity/search?scope=Sample&query=" + sLUID
		searchURI = searchURI.replace( "http://", "https://" )
		searchURI = searchURI.replace( ":8080", "" )

		line = "<tr> <td><a href='%s'>%s</a></td> <td>%s</td> <td>%s</td> <td>%s</td> <td>%s</td> </tr>" % (searchURI, sName, pName, pLUID, analysis, refGenome)
		#print line
		TABLE.append( line )

	TABLE.append( "</tbody></table>" )
	TEXT.append( "<p>The responsible contact for this sequencing run is: %s %s (%s).</p>" % (rFirstName, rLastName, rEmail) )
	SUBJECT = "A "+options.machine+" run for project(s) "+",".join(pNames)+" ("+ ",".join(pIDs)+") was just started on "+cName

	sendMessage( SUBJECT, "\r\n".join( TEXT + TABLE ) )

def main():

	global api
	global options

	parser = OptionParser()
	parser.add_option( "-u", "--username", help = "username of the current user", action = 'store', dest = 'username' )
	parser.add_option( "-p", "--password", help = "password of the current user" )
	parser.add_option( "-l", "--limsid", help = "the limsid of the process under investigation" )
	parser.add_option( "-s", "--stepURI", help = "the URI of the step that launched this script" )
	parser.add_option( "-m", "--machine", help = "machine used for sequencing") 

	(options, otherArgs) = parser.parse_args()

	setupGlobalsFromURI( options.stepURI )
	api = glsapiutil.glsapiutil()
	api.setHostname( HOSTNAME )
	api.setVersion( VERSION )
	api.setup( options.username, options.password )

	## at this point, we have the parameters the EPP plugin passed, and we have network plumbing
	## so let's get this show on the road!
	buildMessage()

if __name__ == "__main__":
	main()
