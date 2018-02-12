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

DEBUG = True
api = None
args = None

configDict = {}
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
print __location__
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

def readConfig():
	# config file format needs to be tab delimited with columns of Last,First, and Groups.
	# The groups that a user is part of need to be separated by a comma (no space)
	with open(os.path.join(__location__,'group_permissions_config.txt')) as f:
		lines = f.readlines()
	for line in lines:
		line = line.strip()
		tokens = line.split("\t")
		configDict[tokens[0],tokens[1]]=tokens[2]

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
	readConfig()
	try:
		# is the technicians name a key in the dictionary created from the config file
		# if so find the groups the techician has been assigned in the config
		config_groups = (configDict[first,last]).split(",")
		step_approved = [y.strip() for y in (args["groups"].split(","))]
		if bool(set(config_groups) & set(step_approved)) is False:
		#fail script, stop user from moving forward in the step and have the last last print statement appear in message box
			print "User %s %s has not been approved to run steps in this protocol." % ( first, last )
			exit (-1)

	except:
		print "This technician's name has not been included in the config file "
		exit (-1)


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
