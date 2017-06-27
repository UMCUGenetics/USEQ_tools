import glsapiutil
import xml.dom.minidom
import smtplib
import urllib
import os
import re
import time
#from genologics.lims import *
from genologics.config import BASEURI, USERNAME, PASSWORD
from datetime import datetime
from xml.dom.minidom import parseString
from optparse import OptionParser

API_USER=USERNAME
API_PW=PASSWORD
API_URI=BASEURI+'/api/v2/'

HOSTNAME = ''
VERSION = ''
BASE_URI = ''

DEBUG = False
api = None
CACHE = {}
PROJECT_INFO = {}


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




def getProjectIDS( ):

    pl_XML = api.getResourceByURI( BASE_URI + 'projects' )
    pl_DOM = parseString( pl_XML )
    
    project_ids = []
    
    #While there is a next project page continue
    while pl_DOM.getElementsByTagName('next-page'):
	project_nodes = pl_DOM.getElementsByTagName( "project" )
	for project_node in project_nodes:
	    project_id = project_node.getAttribute( "limsid" )
	    project_ids.append( project_id )
	
	next_page = pl_DOM.getElementsByTagName('next-page')[0].getAttribute( "uri" )
	pl_XML = api.getResourceByURI( next_page )
	pl_DOM = parseString( pl_XML )
    #Last project page
    else:
	project_nodes = pl_DOM.getElementsByTagName( "project" )
	for project_node in project_nodes:
	    project_id = project_node.getAttribute( "limsid" )
	    project_ids.append( project_id )
    
    return project_ids
	
	
	
def getSampleByID( sample_id ):
    sample = {
	'name':'',
	'avg_library_size' : '',
	'libprep_kit' : ''
    }
    
    s_XML = api.getResourceByURI( BASE_URI + 'samples/' + sample_id )
    s_DOM = parseString( s_XML )
    
    sample['name'] = s_DOM.getElementsByTagName( "name" )[0].firstChild.data
    sample['avg_library_size'] = api.getUDF( s_DOM, "Fragment Size (bp)" )
    sample['libprep_kit'] = api.getUDF( s_DOM, "Library prep kit")

    return sample

def getProjectByID( project_id ):
    project = {
	'name' : '',
	'account' : '',
	'contact' : '',
	'avg_library_size' : '',
	'libprep_kit' : '',
	'qPCR_values' : {'ct08':'', 'ct12':'', 'ct16':''},
	'average_length' : '',
	'loading_conc' : ''
    }
    
    p_XML = api.getResourceByURI( BASE_URI + 'projects/' + project_id)
    p_DOM = parseString( p_XML )

    project['name'] = p_DOM.getElementsByTagName( "name" )[0].firstChild.data
    
    researcher_uri = p_DOM.getElementsByTagName( "researcher" )[0].getAttribute( "uri" )
    r_DOM = getObjectDOM( researcher_uri )
    researcher_fname = r_DOM.getElementsByTagName( "first-name" )[0].firstChild.data
    researcher_lname = r_DOM.getElementsByTagName( "last-name" )[0].firstChild.data
    project['contact'] = researcher_fname + ' ' + researcher_lname
    
    account_uri = r_DOM.getElementsByTagName( "lab" )[0].getAttribute( "uri" )
    a_DOM = getObjectDOM( account_uri )
    project['account'] = a_DOM.getElementsByTagName( "name" )[0].firstChild.data
    
    
    sl_XML = api.getResourceByURI( BASE_URI + 'samples/?projectlimsid=' + project_id )
    sl_DOM = parseString( sl_XML )
    
    
    if sl_DOM.getElementsByTagName( "sample" ):
	first_sample_id = sl_DOM.getElementsByTagName( "sample" )[0].getAttribute( "limsid" )
	sample = getSampleByID( first_sample_id )
	project['avg_library_size'] = sample['avg_library_size']
	project['libprep_kit'] = sample['libprep_kit']
    
	#al_XML = api.getResourceByURI( BASE_URI + 'artifacts/?process-type=' + urllib.quote( 'qPCR QC 5.0' ) + '&type=ResultFile&qc-flag=PASSED&samplelimsid=' + first_sample_id )
	al_XML = api.getResourceByURI( BASE_URI + 'artifacts/?process-type=' + urllib.quote( 'qPCR QC 5.0' ) + '&type=ResultFile&samplelimsid=' + first_sample_id )
	al_DOM = parseString( al_XML )
    
	qc_artifacts = al_DOM.getElementsByTagName( "artifact" )
    
	if qc_artifacts:
	    for qc_artifact in qc_artifacts:
		qc_artifact_id = qc_artifact.getAttribute( "limsid" )
		a_XML = api.getResourceByURI( BASE_URI + 'artifacts/' + qc_artifact_id )
		a_DOM = parseString( a_XML )
		
		if api.getUDF( a_DOM, "Ct 08 pM" ):
		    project['qPCR_values']['ct08'] = api.getUDF( a_DOM, "Ct 08 pM" )
		    project['qPCR_values']['ct12'] = api.getUDF( a_DOM, "Ct 12 pM" )
		    project['qPCR_values']['ct16'] = api.getUDF( a_DOM, "Ct 16 pM" )
		    break
	
	al_XML = api.getResourceByURI( BASE_URI + 'artifacts/?type=Analyte&samplelimsid=' + first_sample_id )
	al_DOM = parseString( al_XML )
	
	analyte_artifacts = al_DOM.getElementsByTagName( "artifact" )

	
	if analyte_artifacts:
	    for analyte_artifact in analyte_artifacts:
		analyte_artifact_id = analyte_artifact.getAttribute( "limsid" )
		a_XML = api.getResourceByURI( BASE_URI + 'artifacts/' + analyte_artifact_id )
		a_DOM = parseString( a_XML )
		
		if api.getUDF( a_DOM, "Average length (bp)"):
		    project['average_length'] = api.getUDF( a_DOM, "Average length (bp)" )
		if api.getUDF( a_DOM, "Loading Conc. (pM)" ):
		    project['loading_conc'] = api.getUDF( a_DOM, "Loading Conc. (pM)" )
		
    return project
    
    
    
    



def main():

    global api    

    setupGlobalsFromURI( API_URI )
    api = glsapiutil.glsapiutil()
    api.setHostname( HOSTNAME )
    api.setVersion( VERSION )
    api.setup( API_USER, API_PW )

    project_ids = getProjectIDS()
    print "Project_ID\tProject_Name\tAccount\tContact\tAverage Library Size\tLibrary Prep Kit\tqPCR Ct 08 pM\tqPCR Ct 12 pM\tqPCR Ct 16 pM\tAverage length (bp)\tLoading Conc. (pM)"
    for project_id in project_ids:
	project = getProjectByID( project_id )
	
	print project_id + "\t" + "\t".join( [project['name'],project['account'],project['contact'],project['avg_library_size'],project['libprep_kit'],"\t".join([project['qPCR_values']['ct08'],project['qPCR_values']['ct12'],project['qPCR_values']['ct16']]),project['average_length'], project['loading_conc']] )
    



if __name__ == "__main__":
    main()