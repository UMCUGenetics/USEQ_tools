__author__ = 'travis.glennon'

import sys
import getopt
import glsapiutil
import os
from xml.dom.minidom import parseString

HOSTNAME = ""
VERSION = ""
BASE_URI = ""
limsid = ""

DEBUG = False
api = None
args = None

def setupGlobalsFromURI( uri ):

	global HOSTNAME
	global VERSION
	global BASE_URI
	global limsid

	tokens = uri.split( "/" )
	HOSTNAME = "/".join(tokens[0:3])
	VERSION = tokens[4]
	BASE_URI = "/".join(tokens[0:5]) + "/"
	limsid = (tokens[-1])


	if DEBUG is True:
		print HOSTNAME
		print BASE_URI


def findResearcherFromProcess():
	## get the XML for the process
	#step_researcher_uri
	pURI = BASE_URI + "processes/" + limsid
	pXML = api.getResourceByURI(pURI)
	pDOM = parseString(pXML)
	nodes = pDOM.getElementsByTagName("technician")
	step_researcher_uri = nodes[0].getAttribute("uri")
	return step_researcher_uri


def checkResearcher(rURI):
	rXML = api.getResourceByURI(rURI)
	rDOM = parseString(rXML)
	nodes = rDOM.getElementsByTagName( "first-name" )
	first = nodes[0].firstChild.data
	nodes = rDOM.getElementsByTagName( "last-name" )
	last = nodes[0].firstChild.data

	lab_uri = rDOM.getElementsByTagName( "lab" )[0].getAttribute("uri")
	lXML = api.getResourceByURI(lab_uri)
	lDOM = parseString(lXML)
	lab_name = lDOM.getElementsByTagName( "name" )[0].firstChild.data

	groups = args[ "groups" ].split(",")
	if lab_name not in groups:
		print "User %s %s in %s has not been approved to run steps in this protocol." % ( first, last, lab_name )
		exit(-1)



def main():
	global api
	global args

	args = {}

	opts, extraparams = getopt.getopt(sys.argv[1:], "u:p:s:g:")

	for o, p in opts:
		if o == '-u':
			args["username"] = p
		elif o == '-p':
			args["password"] = p
		elif o == '-s':
			args["stepURI"] = p
		elif o == '-g':
			args["groups"] = p

	setupGlobalsFromURI( args[ "stepURI" ] )
	api = glsapiutil.glsapiutil()
	api.setHostname(HOSTNAME)
	api.setVersion(VERSION)
	api.setup(args["username"], args["password"])



	## at this point, we have the parameters the EPP plugin passed, and we have network plumbing
	## so let's get this show on the road!

	step_researcher_uri = findResearcherFromProcess()
	checkResearcher(step_researcher_uri)


if __name__ == "__main__":
	main()
