import time
import smtplib
import glsapiutil
import xml.dom.minidom
import urllib
import sys
from optparse import OptionParser
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
import re
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

    outer = MIMEMultipart()
    outer[ "Subject" ] = 'USEQ announcement: '+ str(options.subject)
    outer[ "From" ] = options.mail

    contents = ""

    # header = MIMEText('<p>Dear USEQ user,</p>', 'html')
    contents += "<p>Dear USEQ user,</p>"
    # outer.attach(header)

    fp = open(mailInfo)

    # message = MIMEText( fp.read(), 'html' )
    # outer.attach(message)
    message = fp.read()
    contents += message

    logo = 'resources/useq_logo.jpg'
    logo_name = 'useq_logo.jpg'

    footer_html = "<p>Kind regards,</p>"
    footer_html += "<p>The USEQ team</p><img src='cid:logo_image' style='width:231;height:80;'><p>"
    footer_html += "<i>Utrecht Sequencing Facility (USEQ) | Joint initiative of the University Medical Center Utrecht, "
    footer_html += "Hubrecht Institute and Utrecht University Center for Molecular Medicine | UMC Utrecht | room STR2.207 | "
    footer_html += "Heidelberglaan 100 | 3584 CX Utrecht | The Netherlands | Tel: +31 (0)88 75 55164 | "
    footer_html += "<a href='mailto:USEQ@umcutrecht.nl'>USEQ@umcutrecht.nl</a> | <a href='www.USEQ.nl'>www.USEQ.nl</a></i></p>"
    # footer = MIMEText( footer_html, 'html')
    contents += footer_html

    contents = MIMEText( contents, 'html')
    outer.attach( contents )

    #read the logo and add it to the email
    fp = open(logo, 'rb')
    logo_image = MIMEImage(fp.read())
    fp.close()
    logo_image.add_header('Content-ID', '<logo_image>')
    outer.attach(logo_image)


    print "You're about to send the following email :"
    print "Subject : USEQ announcement: " + str(options.subject)
    print "Content : \n" + message
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
            s.sendmail( options.mail, email, outer.as_string() )
            s.quit()
            time.sleep(1)




def parseFile ( file ):

    return file

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
    parser.add_option( "-m", "--mail", help = "Email address of email sender.", default='useq@umcutrecht.nl' )
    parser.add_option( "-s", "--subject", help = "Subject of email.")
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

    if not options.subject:
        parser.error('No subject given')

    mailInfo = parseFile( options.file )
    emailAddresses = getEmailAddresses( options.all, options.group, options.user)

    sendMails( mailInfo, emailAddresses )

if __name__ == "__main__":
    main()
