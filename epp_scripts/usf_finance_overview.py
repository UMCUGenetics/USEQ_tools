import glsapiutil
import xml.dom.minidom
import smtplib
import codecs

from xml.dom.minidom import parseString
from optparse import OptionParser
from email.mime.text import MIMEText

HOSTNAME = ''
VERSION = ''
BASE_URI = ''

DEBUG = True
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

def getUniqueUDF( objects, udf_name ):
    udf_set = set([])
    for object in objects:
        udf_value = api.getUDF(object, udf_name)
        if udf_value:
            udf_set.add(udf_value)

    if len(udf_set) == 1:
        return udf_set.pop()
    elif len(udf_set) > 2:
        return "multiple"
    else:
        return None


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

def getProject( projectID ):

    return getObjectDOM( BASE_URI + "projects/" + projectID )


def getResearcher( uri ):
    
    return getObjectDOM( uri )
    
    
def getLab( uri ):

    return getObjectDOM( uri)

def getSeqFinance() :
    
    seqFinance = []
    

    stepURI = options.stepURI + "/details"
    stepXML = api.getResourceByURI( stepURI )
    stepDOM = parseString( stepXML )
    
    #Get the input analytes lims ids
    analyteIDS = []
    for input in stepDOM.getElementsByTagName( "input" ):
	analyteID = input.getAttribute( "limsid" )
	
	if analyteID not in analyteIDS:
	    analyteIDS.append( analyteID )
    
    #Get the input artifacts (which is a pool of samples) by lims id
    artifacts = getArtifacts( analyteIDS )
    
    #Get sample information per pool
    for artifact in artifacts:
	####TOOOODOOOOO#####
	seq_costs = 0
	prep_costs = 0
	iso_costs = 0
	t_costs = 0
	####################
	errors = []
	t_fail = 0
	t_seq = 0
	n_isolated = 0
	n_prepped = 0
	reagent_list = []
	process_counts = {}
    
	samples = []
	sampleIDS = []
	for sample in artifact.getElementsByTagName( "sample" ):
	    sampleID = sample.getAttribute( "limsid" )
	    
	    if sampleID not in sampleIDS:
		sampleIDS.append( sampleID )
	
	
	samples = getSamples( sampleIDS )
	
	for sample in samples:
	    sampleType = api.getUDF( sample, 'Sample Type' )
	    if sampleType is None:
		errors.append( "Sample without 'Sample Type' found" )
	    elif sampleType.endswith( "unisolated" ):
		n_isolated += 1
		n_prepped += 1
	    elif sampleType.endswith( "isolated" ):
		n_prepped += 1

	
	project = getProject( samples[0].getElementsByTagName( "project" )[0].getAttribute( "limsid" ) )
	project_name = project.getElementsByTagName( "name" )[0].firstChild.data
	
	
	
	
	researcher = getResearcher( project.getElementsByTagName( "researcher" )[0].getAttribute( "uri" ) )
	researcher_fname = researcher.getElementsByTagName( "first-name" )[0].firstChild.data
	researcher_lname = researcher.getElementsByTagName( "last-name" )[0].firstChild.data
	researcher_name = researcher_fname+" "+researcher_lname
	
	lab = getLab( researcher.getElementsByTagName( "lab" )[0].getAttribute( "uri" ) )
	lab_name = lab.getElementsByTagName( "name" )[0].firstChild.data
	billing_address = lab.getElementsByTagName( "billing-address" )[0]

	
	seqFinance.append(
	    u"{errors}\t{pool_name}\t{project_name}\t{id}\t{open_date}\t{contact_name}\t{contact_email}\t{sequencing_runtype}\t{sequencing_succesful}\t{requested_analysis}\t{nr_samples}\t{nr_samples_prepped}\t{nr_samples_isolated}\t{sample_type}\t{library_prep_kit}\t{account}\t{project_budget_number}\t{sequencing_costs}\t{library_prep_costs}\t{isolation_costs}\t{total_costs}\t{billing_institute}\t{billing_postalcode}\t{billing_city}\t{billing_country}".format(
		errors = ','.join( set(errors) ),
		pool_name = artifact.getElementsByTagName( "name" )[0].firstChild.data,
		project_name = project_name,
		id = project.getElementsByTagName( "prj:project" )[0].getAttribute( "limsid" ),
		open_date = project.getElementsByTagName( "open-date" )[0].firstChild.data,
		contact_name = researcher_name,
		contact_email = researcher.getElementsByTagName( "email" )[0].firstChild.data,
		sequencing_runtype = getUniqueUDF( samples,'Sequencing Runtype'),
		sequencing_succesful = api.getUDF( artifact, 'Sequencing Succesful' ),
		requested_analysis = getUniqueUDF( samples,'Analysis'),
		nr_samples = len(samples),
		nr_samples_prepped = n_prepped,
		nr_samples_isolated = n_isolated,
		sample_type = getUniqueUDF( samples,'Sample Type'),
		library_prep_kit = getUniqueUDF( samples,'Library prep kit'),
		account = lab_name,
		project_budget_number = getUniqueUDF( samples,'Budget Number'),
		sequencing_costs = seq_costs,
		library_prep_costs = prep_costs,
		isolation_costs = iso_costs,
		total_costs = t_costs,
		billing_institute = billing_address.getElementsByTagName( "institution" )[0].firstChild.data,
		billing_postalcode = billing_address.getElementsByTagName( "postalCode" )[0].firstChild.data,
		billing_city = billing_address.getElementsByTagName( "city" )[0].firstChild.data,
		billing_country = billing_address.getElementsByTagName( "country" )[0].firstChild.data
	    )
	)
    return seqFinance
	

def main():

	global api
	global options

	parser = OptionParser()
	parser.add_option( "-u", "--username", help = "username of the current user", action = 'store', dest = 'username' )
	parser.add_option( "-p", "--password", help = "password of the current user" )
	parser.add_option( "-s", "--stepURI", help = "the URI of the step that launched this script" )
	parser.add_option( "-o", "--outname", help = "Output file name + path" )
	
	(options, otherArgs) = parser.parse_args()

	setupGlobalsFromURI( options.stepURI )
	api = glsapiutil.glsapiutil()
	api.setHostname( HOSTNAME )
	api.setVersion( VERSION )
	api.setup( options.username, options.password )

	## at this point, we have the parameters the EPP plugin passed, and we have network plumbing
	## so let's get this show on the road!
	seqFinanceTable = getSeqFinance()
	#print "\n".join( seqFinanceTable )
	
	seq_finance = codecs.open(options.outname, 'w', 'utf-8')
	seq_finance.write(u"errors\tpool_name\tproject_name\tid\topen_date\tcontact_name\tcontact_email\tsequencing_runtype\tsequencing_succesful\trequested_analysis\tnr_samples\tnr_samples_prepped\tnr_samples_isolated\tsample_type\tlibrary_prep_kit\taccount\tproject_budget_number\tsequencing_costs\tlibrary_prep_costs\tisolation_costs\ttotal_costs\tbilling_institute\tbilling_postalcode\tbilling_city\tbilling_country")
	seq_finance.write( "\n".join( seqFinanceTable ) )
	seq_finance.close()
	
if __name__ == "__main__":
	main()