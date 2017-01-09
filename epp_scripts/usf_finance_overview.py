import glsapiutil
import xml.dom.minidom
import smtplib
import codecs
import json

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
    elif len(udf_set) >= 2:
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

    return getObjectDOM( uri )

def getContainer( uri ):

    return getObjectDOM( uri )

def getAllCosts( uri ):

    costJSON = api.getResourceByURI( uri )
    return json.loads( costJSON )

def getRunMode( uri ):
    stepURI = options.stepURI + "/details"
    stepXML = api.getResourceByURI( stepURI )
    stepDOM = parseString( stepXML )
    step_uri = stepDOM.getElementsByTagName( "step" )[0].getAttribute( "uri" )

#    print step_uri

    step = getObjectDOM( step_uri )
    configuration_uri = step.getElementsByTagName( "configuration" )[0].getAttribute( "uri" )
#    print configuration_uri
    configuration = getObjectDOM( configuration_uri )
    
    protocol_uri = configuration.getElementsByTagName( "protstepcnf:step" )[0].getAttribute( "protocol-uri" )
#    print protocol_uri
    protocol = getObjectDOM( protocol_uri )
    
    workflow_name = protocol.getElementsByTagName( "protcnf:protocol" )[0].getAttribute( "name" )
    return workflow_name
    
    
def getSnpFinance() :
    
    snpFinance = []
    
    allCosts = getAllCosts( 'http://www.useq.nl/useq_getfinance.php?type=all&mode=json' )

    stepURI = options.stepURI + "/details"
    stepXML = api.getResourceByURI( stepURI )
    stepDOM = parseString( stepXML )
    
    #Get the input analytes lims ids
    analyteIDS = []
    for input in stepDOM.getElementsByTagName( "input" ):
	analyteID = input.getAttribute( "limsid" )
	#print analyteID
	if analyteID not in analyteIDS:
	    analyteIDS.append( analyteID )
    
    #Get the input artifacts (which can be a pool samples) by lims id
    artifacts = getArtifacts( analyteIDS )
    parent_artifact_ids = []
    
    #Get all parent artifacts 
    for artifact in artifacts:
	parent_process_uri = artifact.getElementsByTagName( "parent-process" )[0].getAttribute( "uri" )
	parent_process = getObjectDOM( parent_process_uri )
	
	for parent_artifact in parent_process.getElementsByTagName( "input" ):
	    aid = parent_artifact.getAttribute("limsid")
	    if aid not in parent_artifact_ids:
		parent_artifact_ids.append( aid )
	    
    parent_artifacts = getArtifacts(parent_artifact_ids)
    
    

    for artifact in parent_artifacts:
    	samples = []
	sampleIDS = []
	snp_costs = 0
	iso_costs = 0
	errors = []
	isolated = 'no'
	
	

	container_uri = artifact.getElementsByTagName( "container" )[0].getAttribute( "uri" )
	container = getContainer( container_uri )
	container_name = container.getElementsByTagName( "name" )[0].firstChild.data
	#print container_name
	
	for sample in artifact.getElementsByTagName( "sample" ):
	    sampleID = sample.getAttribute( "limsid" )
	    #print sampleID
	    if sampleID not in sampleIDS:
		sampleIDS.append( sampleID )
		
	
	
	samples = getSamples( sampleIDS )
	sampleDateReceived = ''
        sample_name = ''
        
	for sample in samples:
	    sampleType = api.getUDF( sample, 'Sample Type' )
	    sample_name = sample.getElementsByTagName( "name" )[0].firstChild.data
	    sampleDateReceived = sample.getElementsByTagName( "date-received" )[0].firstChild.data
	    if sampleType is None:
		errors.append( "Sample without 'Sample Type' found" )
	    elif sampleType.endswith( "unisolated" ):
		isolated = 'yes'

	project = getProject( samples[0].getElementsByTagName( "project" )[0].getAttribute( "limsid" ) )
	project_name = project.getElementsByTagName( "name" )[0].firstChild.data
		    
	researcher = getResearcher( project.getElementsByTagName( "researcher" )[0].getAttribute( "uri" ) )
	researcher_fname = researcher.getElementsByTagName( "first-name" )[0].firstChild.data
	researcher_lname = researcher.getElementsByTagName( "last-name" )[0].firstChild.data
	researcher_name = researcher_fname+" "+researcher_lname
	
	
	lab = getLab( researcher.getElementsByTagName( "lab" )[0].getAttribute( "uri" ) )
	lab_name = lab.getElementsByTagName( "name" )[0].firstChild.data
	billing_address = lab.getElementsByTagName( "billing-address" )[0]
	billingDate = None
	#print container_name, project_name, researcher_name, billing_address
	
	###Calculate run costs
	#sample_type = getUniqueUDF( samples,'Sample Type')
	#library_prep_kit = getUniqueUDF( samples,'Library prep kit')
	#sequencing_runtype = getUniqueUDF( samples,'Sequencing Runtype')
	#billingDate = None
	#print allCosts
	#for key in allCosts:
	#    print key
	
	for date in sorted( allCosts[ 'Open SNP Array' ][ 'date_costs'].keys() ):
	    if date <= sampleDateReceived :
		billingDate = date
	
	#Contingency for if samples were recieved before implementation of cost database
	if billingDate is None:
	    billingDate = sorted( allCosts[ 'Open SNP Array' ][ 'date_costs'].keys())[0]
	    errors.append("Could not find a billing date matching the sample recieved date")
	
	plate_costs = allCosts[ 'Open SNP Array' ]['date_costs'][ billingDate]

	###Calculate library prep costs
#	if library_prep_kit in allCosts :
    
#	    prep_costs = int( allCosts[library_prep_kit][ 'date_costs'][ billingDate ] ) * n_prepped
#	elif n_prepped > 0:
#	    errors.append("Could not find library prep kit in billing database")
	
	###Calculate isolation costs
	if sampleType == 'DNA unisolated':
	    iso_costs = int( allCosts['DNA isolation'][ 'date_costs' ][ billingDate ] )
	elif sampleType == 'RNA unisolated':
	    iso_costs = int( allCosts['RNA isolation'][ 'date_costs' ][ billingDate ] )
	
	
	#t_costs = int(seq_costs) + int(iso_costs)

	
	snpFinance.append(u"{errors}\t{container_name}\t{project_name}\t{id}\t{sample_name}\t{open_date}\t{contact_name}\t{contact_email}\t{isolated}\t{sample_type}\t{account}\t{project_budget_number}\t{plate_costs}\t{isolation_costs}\t{billing_institute}\t{billing_postalcode}\t{billing_city}\t{billing_country}\t{billing_department}\t{billing_street}".format(


		errors = ','.join( set(errors) ),
		container_name = container_name,
		project_name = project_name,
		id = project.getElementsByTagName( "prj:project" )[0].getAttribute( "limsid" ),
		sample_name = sample_name,
		open_date = project.getElementsByTagName( "open-date" )[0].firstChild.data,
		contact_name = researcher_name,
		contact_email = researcher.getElementsByTagName( "email" )[0].firstChild.data,
		isolated = isolated,
		sample_type = sampleType,
		account = lab_name,
		project_budget_number = getUniqueUDF( samples,'Budget Number'),
		plate_costs = plate_costs,
		isolation_costs = iso_costs,
		billing_institute = billing_address.getElementsByTagName( "institution" )[0].firstChild.data,
		billing_postalcode = billing_address.getElementsByTagName( "postalCode" )[0].firstChild.data,
		billing_city = billing_address.getElementsByTagName( "city" )[0].firstChild.data,
		billing_country = billing_address.getElementsByTagName( "country" )[0].firstChild.data,
		billing_department = billing_address.getElementsByTagName( "department" )[0].firstChild.data,
		billing_street = billing_address.getElementsByTagName( "street" )[0].firstChild.data
				 
	    )
	)

    return snpFinance
	
def getSeqFinance() :
    
    seqFinance = []
    
    allCosts = getAllCosts( 'http://www.useq.nl/useq_getfinance.php?type=all&mode=json' )
    #for step in allCosts:
	#print step
    

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
	sampleDateReceived = ''
	
	for sample in samples:
	    sampleType = api.getUDF( sample, 'Sample Type' )
	    sampleDateReceived = sample.getElementsByTagName( "date-received" )[0].firstChild.data
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

	###Calculate run costs
	sample_type = getUniqueUDF( samples,'Sample Type')
	library_prep_kit = getUniqueUDF( samples,'Library prep kit')
	sequencing_runtype = getUniqueUDF( samples,'Sequencing Runtype')
	billingDate = None
	
	for date in sorted( allCosts[ sequencing_runtype ][ 'date_costs'].keys() ):
	    if date <= sampleDateReceived :
		billingDate = date
		
	#Contingency for if samples were recieved before implementation of cost database
	if billingDate is None:
	    billingDate = sorted( allCosts[ sequencing_runtype ][ 'date_costs'].keys())[0]
	    errors.append("Could not find a billing date matching the sample recieved date")
	
	seq_costs = allCosts[ sequencing_runtype ]['date_costs'][ billingDate]

	###Calculate library prep costs
	if library_prep_kit in allCosts :
    
	    prep_costs = int( allCosts[library_prep_kit][ 'date_costs'][ billingDate ] ) * n_prepped
	elif n_prepped > 0:
	    errors.append("Could not find library prep kit in billing database")
	
	###Calculate isolation costs
	if sampleType == 'DNA unisolated':
	    iso_costs = int( allCosts['DNA isolation'][ 'date_costs' ][ billingDate ] ) * n_isolated
	elif sampleType == 'RNA unisolated':
	    iso_costs = int( allCosts['RNA isolation'][ 'date_costs' ][ billingDate ] ) * n_isolated
	
	
	t_costs = int(seq_costs) + int(prep_costs) + int(iso_costs)
	
	seqFinance.append(
	    u"{errors}\t{pool_name}\t{project_name}\t{id}\t{open_date}\t{contact_name}\t{contact_email}\t{sequencing_runtype}\t{sequencing_succesful}\t{requested_analysis}\t{nr_samples}\t{nr_samples_prepped}\t{nr_samples_isolated}\t{sample_type}\t{library_prep_kit}\t{account}\t{project_budget_number}\t{sequencing_costs}\t{library_prep_costs}\t{isolation_costs}\t{total_costs}\t{billing_institute}\t{billing_postalcode}\t{billing_city}\t{billing_country}\t{billing_department}\t{billing_street}".format(
		errors = ','.join( set(errors) ),
		pool_name = artifact.getElementsByTagName( "name" )[0].firstChild.data,
		project_name = project_name,
		id = project.getElementsByTagName( "prj:project" )[0].getAttribute( "limsid" ),
		open_date = project.getElementsByTagName( "open-date" )[0].firstChild.data,
		contact_name = researcher_name,
		contact_email = researcher.getElementsByTagName( "email" )[0].firstChild.data,
		sequencing_runtype = sequencing_runtype,
		sequencing_succesful = api.getUDF( artifact, 'Sequencing Succesful' ),
		requested_analysis = getUniqueUDF( samples,'Analysis'),
		nr_samples = len(samples),
		nr_samples_prepped = n_prepped,
		nr_samples_isolated = n_isolated,
		sample_type = sample_type,
		library_prep_kit = library_prep_kit,
		account = lab_name,
		project_budget_number = getUniqueUDF( samples,'Budget Number'),
		sequencing_costs = seq_costs,
		library_prep_costs = prep_costs,
		isolation_costs = iso_costs,
		total_costs = t_costs,
		billing_institute = billing_address.getElementsByTagName( "institution" )[0].firstChild.data,
		billing_postalcode = billing_address.getElementsByTagName( "postalCode" )[0].firstChild.data,
		billing_city = billing_address.getElementsByTagName( "city" )[0].firstChild.data,
		billing_country = billing_address.getElementsByTagName( "country" )[0].firstChild.data,
		billing_department = billing_address.getElementsByTagName( "department" )[0].firstChild.data,
		billing_street = billing_address.getElementsByTagName( "street" )[0].firstChild.data
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
	
	#Determine run mode
	mode = getRunMode( options.stepURI )
	#print mode
	
	if mode == 'USF Post Sequencing steps':
	    seqFinanceTable = getSeqFinance()
	    #print "\n".join( seqFinanceTable )
	
	    seq_finance = codecs.open(options.outname, 'w', 'utf-8')
	    seq_finance.write(u"errors\tpool_name\tproject_name\tid\topen_date\tcontact_name\tcontact_email\tsequencing_runtype\tsequencing_succesful\trequested_analysis\tnr_samples\tnr_samples_prepped\tnr_samples_isolated\tsample_type\tlibrary_prep_kit\taccount\tproject_budget_number\tsequencing_costs\tlibrary_prep_costs\tisolation_costs\ttotal_costs\tbilling_institute\tbilling_postalcode\tbilling_city\tbilling_country\tbilling_department\tbilling_street\n")
	    seq_finance.write( "\n".join( seqFinanceTable ) )
	    seq_finance.close()
	elif mode == 'USF Post Fingerprinting steps':
	    snpFinanceTable = getSnpFinance()
	    snp_finance = codecs.open(options.outname, 'w', 'utf-8')
	    snp_finance.write(u"errors\tcontainer_name\tproject_name\tid\tsample_name\topen_date\tcontact_name\tcontact_email\tisolated\tsample_type\taccount\tproject_budget_number\tplate_costs\tisolation_costs\tbilling_institute\tbilling_postalcode\tbilling_city\tbilling_country\tbilling_department\tbilling_street\n")
	    snp_finance.write( "\n".join( snpFinanceTable ) )
	    snp_finance.close()
	
if __name__ == "__main__":
	main()