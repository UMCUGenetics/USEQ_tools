import smtplib
import glsapiutil
import xml.dom.minidom
import urllib
import sys
from optparse import OptionParser
from email.mime.text import MIMEText
#from genologics.lims import *
from genologics.config import BASEURI, USERNAME, PASSWORD
from xml.dom.minidom import parseString

options = None

API_USER=USERNAME
API_PW=PASSWORD
API_URI=BASEURI+'/api/v2/'

HOSTNAME = ''
VERSION = ''
BASE_URI = ''

DEBUG = False
api = None
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



def sendMails ( mailInfo, emailAddresses ):
    TEXT = []    
    for line in mailInfo:
	TEXT.append( "<p>%s</p>" % (line) )
	
    
    message = MIMEText( "".join( TEXT ), 'html' )

    message[ "Subject" ] = 'USF - notification'
    message[ "From" ] = options.mail

    print "You're about to send the following email :"
    print "Subject : USF - notification"
    print "Content : \n" + "".join( mailInfo )
    print "To:"
    print "\t".join( sorted(emailAddresses) )
    print "Are you sure? Please respond with (y)es or (n)o."
    
    yes = set(['yes','y', 'ye', ''])
    no = set(['no','n'])
    choice = raw_input().lower()
    if choice in yes:
	choice = True
    elif choice in no:
	choice = False
    else:
	sys.stdout.write("Please respond with 'yes' or 'no'")
    
    if choice:
	for email in emailAddresses:
	    s = smtplib.SMTP( "localhost" )
	    s.sendmail( options.mail, email, message.as_string() )
	    s.quit()
	
def parseFile ( file ):    
    f = open( file )
    
    mailInfo = []
    
    for line in f:
	#line = line.rstrip( "\n" )
	mailInfo.append(line)

    f.close
    return mailInfo

def getEmailAddresses(all, group, user):

    addresses = {}

    
    if all :
	r_XML = api.getResourceByURI( BASE_URI + 'researchers')
	r_DOM = parseString( r_XML )
	
	
	researcher_nodes = r_DOM.getElementsByTagName( 'researcher' )
	for researcher_node in researcher_nodes:
	    researcher_uri = researcher_node.getAttribute( 'uri' )
	    
	    researcher_XML = api.getResourceByURI( researcher_uri )
	    researcher_DOM = parseString( researcher_XML )
	    
	    if researcher_DOM.getElementsByTagName( "account-locked" ):
		locked_status = researcher_DOM.getElementsByTagName( "account-locked" )[0].firstChild.data
		#print locked_status
		if locked_status == 'true':
		    continue
	    
	    if researcher_DOM.getElementsByTagName( "email" ):
		researcher_email = researcher_DOM.getElementsByTagName( "email" )[0].firstChild.data
		researcher_email = researcher_email.lower()
		if researcher_email not in addresses:
		    addresses[ researcher_email ] = 1
	
    else:
	if group :
	    groups = group.split(',')
	    lab_uris = []
	    groups = [urllib.quote(x) for x in groups]
	    
	    labs_XML = api.getResourceByURI( BASE_URI + 'labs/?name=' + "&name=".join(groups) )
	    #print BASE_URI + 'labs/?name=' + "&name=".join(groups)
	    labs_DOM = parseString( labs_XML )
	    #print labs_XML
	    
	    lab_nodes = labs_DOM.getElementsByTagName( 'lab' )
	    #print lab_nodes
	    for lab_node in lab_nodes:
		
    		lab_uri = lab_node.getAttribute( 'uri' )
		lab_uris.append( lab_uri )
	    #print lab_uris
	
	    r_XML = api.getResourceByURI( BASE_URI + 'researchers')
	    r_DOM = parseString( r_XML )
	    researcher_nodes = r_DOM.getElementsByTagName( 'researcher' )
	    for researcher_node in researcher_nodes:
		researcher_uri = researcher_node.getAttribute( 'uri' )
		researcher_XML = api.getResourceByURI( researcher_uri )
		researcher_DOM = parseString( researcher_XML )
		
		if researcher_DOM.getElementsByTagName( "account-locked" ):
		    locked_status = researcher_DOM.getElementsByTagName( "account-locked" )[0].firstChild.data
		    if locked_status == 'true':
			continue
		
		researcher_laburi = researcher_DOM.getElementsByTagName( "lab" )[0].getAttribute( 'uri' )
		
		if researcher_laburi in lab_uris:
		    if researcher_DOM.getElementsByTagName( "email" ):
			researcher_email = researcher_DOM.getElementsByTagName( "email" )[0].firstChild.data
			researcher_email = researcher_email.lower()			
			if researcher_email not in addresses:
			    addresses[ researcher_email ] = 1
	
	if user :
	    emails = user.split(",")
	    for email in emails:
		email = email.lower()
		if email not in addresses:
		    addresses[ email ] = 1
	

    return addresses.keys()
    
def main():
    global options
    global api
    
    #PARSE OPTIONS    
    parser = OptionParser()
    parser.add_option( "-f", "--file", help = "Text file containing email contents." )
    parser.add_option( "-s", "--mail", help = "Email address of email sender.", default='usf@umcutrecht.nl' )
    parser.add_option( "-a", "--all", help = "Send email to all USF users. Overrides options -g and -u.", action='store_true')
    parser.add_option( "-g", "--group", help = "Send email to all USF users belonging to 1 account name. Create a comma separated list for multiple groups. Can be used together with -u.")
    parser.add_option( "-u", "--user", help = "Send email to USF user(s). (e.g. -u j.doe@email.com). Can be used together with -g.")
    
    ( options, otherArgs ) = parser.parse_args()
    
    #SETUP API CONNECTION
    setupGlobalsFromURI( API_URI )
    api = glsapiutil.glsapiutil()
    api.setHostname( HOSTNAME )
    api.setVersion( VERSION )
    api.setup( API_USER, API_PW )
    
    if not options.file:
	parser.error('No file argument given')
	
    if not options.group and not options.user and not options.all:
	parser.error('No all or group/user argument given')
	
    mailInfo = parseFile( options.file )
    emailAddresses = getEmailAddresses( options.all, options.group, options.user)

    sendMails( mailInfo, emailAddresses )

if __name__ == "__main__":
    main()