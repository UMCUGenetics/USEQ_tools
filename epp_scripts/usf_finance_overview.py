import glsapiutil
import xml.dom.minidom
import smtplib
import codecs
import json
import urllib2
import re
#import sys
#import pprint
from xml.dom.minidom import parseString
from optparse import OptionParser
from email.mime.text import MIMEText
from xml.parsers.expat import ExpatError
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
		#print uri
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
    costs = json.loads( costJSON )
    
    return dict( (k.lower(), v) for k,v in costs.iteritems())

def getProcesses( uri ):
    
    return getObjectDOM( uri)
    

def getRunTypes( project_name ):
    
    nextseq_run = getObjectDOM( BASE_URI + 'processes/?projectname='+project_name+'&type=NextSeq Run (NextSeq) 1.0')
    miseq_run = getObjectDOM( BASE_URI + 'processes/?projectname='+project_name+'&type=MiSeq Run (MiSeq) 4.0')
    hiseq_run = getObjectDOM( BASE_URI + 'processes/?projectname='+project_name+'&type=HiSeq Run (HiSeq) 5.0')
    
    runtypes = {'nr_nextseq':0, 'nr_miseq':0, 'nr_hiseq':0}
    
    if nextseq_run.getElementsByTagName( "process" ):
	runtypes['nr_nextseq'] = len( nextseq_run.getElementsByTagName( "process" ) )
    
    if hiseq_run.getElementsByTagName( "process" ):
	runtypes['nr_hiseq'] = len( hiseq_run.getElementsByTagName( "process" ) )

    if miseq_run.getElementsByTagName( "process" ):
	runtypes['nr_miseq'] = len( miseq_run.getElementsByTagName( "process" ) )
	
    return runtypes



def getStepWorkflow( uri ):

    step = getObjectDOM( uri )
    configuration_uri = step.getElementsByTagName( "configuration" )[0].getAttribute( "uri" )
    
    protocol_uri = re.sub("\/steps\/\d+","",configuration_uri)
    #print protocol_uri
    protocol = getObjectDOM( protocol_uri )
    workflow_name = protocol.getElementsByTagName( "protcnf:protocol" )[0].getAttribute( "name" )

    return workflow_name
    
def getSampleLibraryprepArtID( sample_id ):
    

    sample_artifacts = api.getResourceByURI( BASE_URI + 'artifacts?samplelimsid='+sample_id + '&process-type='+urllib2.quote('Enrich DNA fragments (TruSeq Nano) 4.0')+'&process-type='+urllib2.quote('Enrich DNA fragments (TruSeq Stranded mRNA) 5.0')+'&process-type='+urllib2.quote('Enrich DNA fragments (TruSeq Stranded Total RNA) 5.0') )

    
    response_text = None
    
    try:
	sample_artifacts_DOM = parseString( sample_artifacts )
    
	if sample_artifacts_DOM.getElementsByTagName( "artifact"):
	    response_text = sample_artifacts_DOM.getElementsByTagName( "artifact" )[0].getAttribute( "limsid" )
    
    except ExpatError:
	response_text = None

    return response_text
	
	

    
    

def getSampleIsolationArtID( sample_id ):
    
    sample_artifacts = api.getResourceByURI( BASE_URI + 'artifacts?samplelimsid='+sample_id + '&process-type='+urllib2.quote('Qiagen genomic tip DNA isolation')+'&process-type='+urllib2.quote('QiaSymphony RNA isolation') )

    response_text = None
    
    try:
	sample_artifacts_DOM = parseString( sample_artifacts )
	if sample_artifacts_DOM.getElementsByTagName( "artifact"):
	    response_text = sample_artifacts_DOM.getElementsByTagName( "artifact" )[0].getAttribute( "limsid" )


    except ExpatError:
	response_text = None
	
    return response_text

    
def getSampleRunTypeArtID( sample_id ):
    
    sample_artifacts = api.getResourceByURI( BASE_URI + 'artifacts?samplelimsid='+sample_id + '&process-type='+urllib2.quote('MiSeq Run (MiSeq) 4.0')+'&process-type='+urllib2.quote('NextSeq Run (NextSeq) 1.0') +'&process-type='+urllib2.quote('HiSeq Run (HiSeq) 5.0') )
    response_text = None
    
    try:
	sample_artifacts_DOM = parseString( sample_artifacts )
	if sample_artifacts_DOM.getElementsByTagName( "artifact"):
	    response_text = sample_artifacts_DOM.getElementsByTagName( "artifact" )[0].getAttribute( "limsid" )
    except ExpatError:
	response_text = None
    
    return response_text
    


def prepareForPrint( counts ):

    toPrint = []
    for n in counts:
	toPrint.append(str(n) + ':' + str(counts[n]))
    #print "TO PRINT:" + ','.join(toPrint)
    return ','.join( toPrint )

def getSnpFinance() :
    
    snpFinance = []
    
    allCosts = getAllCosts( 'http://www.useq.nl/useq_getfinance.php?type=all&mode=json' )

    stepURI = options.stepURI + "/details"
    stepXML = api.getResourceByURI( stepURI )
    #stepDOM = getObjectDOM( stepURI )
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

	

	
	for date in sorted( allCosts[ 'open snp array' ][ 'date_costs'].keys() ):
	    if date <= sampleDateReceived :
		billingDate = date
	
	#Contingency for if samples were recieved before implementation of cost database
	if billingDate is None:
	    billingDate = sorted( allCosts[ 'open snp array' ][ 'date_costs'].keys())[0]
	    errors.append("Could not find a billing date matching the sample recieved date")
	
	plate_costs = allCosts[ 'open snp array' ]['date_costs'][ billingDate]

	###Calculate library prep costs
#	if library_prep_kit in allCosts :
    
#	    prep_costs = int( allCosts[library_prep_kit][ 'date_costs'][ billingDate ] ) * n_prepped
#	elif n_prepped > 0:
#	    errors.append("Could not find library prep kit in billing database")
	
	###Calculate isolation costs
	if sampleType == 'DNA unisolated':
	    iso_costs = int( allCosts['dna isolation'][ 'date_costs' ][ billingDate ] )
	elif sampleType == 'RNA unisolated':
	    iso_costs = int( allCosts['rna isolation'][ 'date_costs' ][ billingDate ] )
	
	
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
    
    seq_finance = []
    
    all_costs = getAllCosts( 'http://www.useq.nl/useq_getfinance.php?type=all&mode=json' )

    #Dict to store all need run info
    sequencing_runs = {}

    stepURI = options.stepURI + "/details"
    stepDOM = getObjectDOM( stepURI )
    #stepXML = api.getResourceByURI( stepURI )
    #stepDOM = parseString( stepXML )
    
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
	pool_id = artifact.getAttribute( "limsid" )
	sequencing_runs[ pool_id ] = {}
	
	#print "ORI",pool_id
    
	sample_ids = []
	for sample in artifact.getElementsByTagName( "sample" ):
	    sample_id = sample.getAttribute( "limsid" )
	    
	    if sample_id not in sample_ids :
		sample_ids.append( sample_id )

	
	sample_metadata = getSamples( sample_ids )
	#isolation_artifact_IDs = {}
	#library_prep_artifact_IDs = {}
	#runtype_artifact_IDs = {}
	isolation_artifact_IDs = []
	library_prep_artifact_IDs = []
	runtype_artifact_IDs = []
	
	for sample in sample_metadata:
	    
	    sample_id = sample.getAttribute( "limsid" )
	    type = api.getUDF( sample, 'Sample Type' )
	    requested_library_prep = api.getUDF( sample, 'Library prep kit' )
	    requested_runtype = api.getUDF( sample, 'Sequencing Runtype')
	    received_date = sample.getElementsByTagName( "date-received" )[0].firstChild.data
	    requested_analysis = api.getUDF( sample,'Analysis')
	    budget_number = api.getUDF( sample,'Budget Number')
	    rootartifact_id = sample.getElementsByTagName( "artifact" )[0].getAttribute( "limsid" )
	    project_id = sample.getElementsByTagName( "project" )[0].getAttribute( "limsid" )
	    
	    si_artid = getSampleIsolationArtID( sample_id )
	    #return seq_finance
	    sl_artid = getSampleLibraryprepArtID( sample_id )
	    sr_artid = getSampleRunTypeArtID( sample_id )

	    if si_artid is not None:
	        #isolation_artifact_IDs[ si_artid ] = pool_id
	        isolation_artifact_IDs.append( si_artid )
	    if sl_artid is not None:
		#library_prep_artifact_IDs[ sl_artid ] = pool_id
		library_prep_artifact_IDs.append( sl_artid )
	    if sr_artid is not None:
		#runtype_artifact_IDs[ sr_artid ] = pool_id
		runtype_artifact_IDs.append( sr_artid )
	    #isolation_artifact_IDs.append( getSampleIsolationArtID( sample_id ) )
	    #library_prep_artifact_IDs.append( getSampleLibraryprepArtID( sample_id) )
	    #runtype_artifact_IDs.append( getSampleRunTypeArtID( sample_id ) )
	    #print 'ID',sample_id,'ISOLATION:',lims_isolation,'LIBPREP',lims_library_prep
    
	    if project_id not in sequencing_runs[ pool_id ] :
		sequencing_runs[ pool_id ][ project_id ] = {
		    'samples' : {}
		}
	    sequencing_runs[ pool_id ][ project_id ][ 'samples' ][ sample_id ] = {
		'received_date' : received_date,
		'type' : type,
		'requested_library_prep': requested_library_prep,
		'requested_runtype' : requested_runtype,
		'requested_analysis' : requested_analysis,
		'lims_isolation' : None,
		'lims_library_prep' : None,
		'lims_runtype' : None
	    }
	    #-->
	    if 'name' not in sequencing_runs[ pool_id ][ project_id ] :
		project_metadata = getProject( project_id )
		project_name = project_metadata.getElementsByTagName( "name" )[0].firstChild.data
		project_open_date = project_metadata.getElementsByTagName( "open-date" )[0].firstChild.data
		sequencing_succesful = api.getUDF( artifact, 'Sequencing Succesful' )
		
		researcher = getResearcher( project_metadata.getElementsByTagName( "researcher" )[0].getAttribute( "uri" ) )
		researcher_fname = researcher.getElementsByTagName( "first-name" )[0].firstChild.data
		researcher_lname = researcher.getElementsByTagName( "last-name" )[0].firstChild.data
		researcher_name = researcher_fname+" "+researcher_lname
		researcher_email = researcher.getElementsByTagName( "email" )[0].firstChild.data
		
		lab = getLab( researcher.getElementsByTagName( "lab" )[0].getAttribute( "uri" ) )
		lab_name = lab.getElementsByTagName( "name" )[0].firstChild.data

		billing_address = lab.getElementsByTagName( "billing-address" )[0]
		billing_institute = billing_address.getElementsByTagName( "institution" )[0].firstChild.data
		billing_postalcode = billing_address.getElementsByTagName( "postalCode" )[0].firstChild.data
		billing_city = billing_address.getElementsByTagName( "city" )[0].firstChild.data
		billing_country = billing_address.getElementsByTagName( "country" )[0].firstChild.data
		billing_department = billing_address.getElementsByTagName( "department" )[0].firstChild.data
		billing_street = billing_address.getElementsByTagName( "street" )[0].firstChild.data

		sequencing_runs[ pool_id ][ project_id ][ 'first_submission_date' ] = received_date
		sequencing_runs[ pool_id ][ project_id ][ 'succesful' ] = sequencing_succesful
		sequencing_runs[ pool_id ][ project_id ][ 'name' ] = project_name
		sequencing_runs[ pool_id ][ project_id ][ 'open_date' ] = project_open_date
		sequencing_runs[ pool_id ][ project_id ][ 'contact_name' ] = researcher_name
		sequencing_runs[ pool_id ][ project_id ][ 'contact_email' ] = researcher_email
		sequencing_runs[ pool_id ][ project_id ][ 'lab_name' ] = lab_name
		sequencing_runs[ pool_id ][ project_id ][ 'budget_nr' ] = budget_number
		sequencing_runs[ pool_id ][ project_id ][ 'institute'] = billing_institute
		sequencing_runs[ pool_id ][ project_id ][ 'postalcode'] = billing_postalcode
		sequencing_runs[ pool_id ][ project_id ][ 'city'] = billing_city
		sequencing_runs[ pool_id ][ project_id ][ 'country'] = billing_country
		sequencing_runs[ pool_id ][ project_id ][ 'department'] = billing_department
		sequencing_runs[ pool_id ][ project_id ][ 'street'] = billing_street
	
    #pp.pprint(isolation_artifact_IDs)
    #pp.pprint(library_prep_artifact_IDs)
    #pp.pprint(runtype_artifact_IDs)
    
	isolation_artifacts = getArtifacts( isolation_artifact_IDs )
	for artifact in isolation_artifacts:
    	    artifact_limsid = artifact.getAttribute("limsid")
    	    #pool_id = isolation_artifact_IDs[ artifact_limsid ]
	
    	    process_limsid = artifact.getElementsByTagName( "parent-process" )[0].getAttribute( "limsid" )
        #print "ISO",process_limsid
    	    isolation = getStepWorkflow( BASE_URI + "steps/" + process_limsid )
    	    for sample in artifact.getElementsByTagName( "sample" ):
		sample_id = sample.getAttribute( "limsid" )

		match = re.search("([A-Z]+\d+)[A-Z]+", sample_id)
		project_id = match.group(1)
		sequencing_runs[ pool_id ][ project_id ][ 'samples' ][ sample_id ]['lims_isolation'] = isolation
		#print "ISO",pool_id,process_limsid, sample_id, project_id
	    
	libraryprep_artifacts = getArtifacts( library_prep_artifact_IDs  )
	for artifact in libraryprep_artifacts:
    	    artifact_limsid = artifact.getAttribute("limsid")
    	    #pool_id = library_prep_artifact_IDs[ artifact_limsid ]
    	    process_limsid = artifact.getElementsByTagName( "parent-process" )[0].getAttribute( "limsid" )
    	    libraryprep = getStepWorkflow( BASE_URI + "steps/" + process_limsid )
    	    #print "LIBPREP",process_limsid
    	    for sample in artifact.getElementsByTagName( "sample" ):
    		sample_id = sample.getAttribute( "limsid" )
		match = re.search("([A-Z]+\d+)[A-Z]+", sample_id)
		project_id = match.group(1)
	    
		sequencing_runs[ pool_id ][ project_id ][ 'samples' ][ sample_id ]['lims_library_prep'] = libraryprep
		#print "LIBPREP",pool_id,process_limsid, sample_id, project_id
	    
	
	runtype_artifacts = getArtifacts( runtype_artifact_IDs )
	for artifact in runtype_artifacts:
    	    artifact_limsid = artifact.getAttribute("limsid")
    	    #pool_id = runtype_artifact_IDs[ artifact_limsid ]
    	    process_limsid = artifact.getElementsByTagName( "parent-process" )[0].getAttribute( "limsid" )
    	    #rint "RUN",process_limsid
    	    runtype = getStepWorkflow( BASE_URI + "steps/" + process_limsid )
    	    for sample in artifact.getElementsByTagName( "sample" ):
    	    
    		sample_id = sample.getAttribute( "limsid" )
		match = re.search("([A-Z]+\d+)[A-Z]+", sample_id)
		project_id = match.group(1)
		sequencing_runs[ pool_id ][ project_id ][ 'samples' ][ sample_id ]['lims_runtype'] = runtype
		#print "RUN",pool_id,process_limsid, sample_id, project_id
    #pp = pprint.PrettyPrinter(indent=4)
    #pp.pprint(sequencing_runs)
    
    
    for run_id in sequencing_runs:
	for project_id in sequencing_runs[ run_id ]:
	
	    seq_costs = 0
	    prep_costs = 0
	    iso_costs = 0
	    t_costs = 0
	    errors = []
	    t_fail = 0
	    t_seq = 0
	    n_isolated = 0
	    n_prepped = 0
	    reagent_list = []
	    process_counts = {}
	    
	    billing_date = None
	    sample_count = 0
	    requested_runtypes = {}
	    requested_librarypreps = {}
	    requested_analyses = {}
	    sample_types = {}
	    lims_runtypes = {}
	    lims_librarypreps = {}
	    lims_isolations = {}
	    
	    for sample_id in sequencing_runs[ run_id ][ project_id ][ 'samples' ]:
		sample_count += 1
		#print run_id, project_id
		#pp.pprint(sequencing_runs[ run_id ][ project_id ][ 'samples' ][ sample_id ])
		requested_runtype = sequencing_runs[ run_id ][ project_id ][ 'samples' ][ sample_id ][ 'requested_runtype' ]
		if requested_runtype in requested_runtypes : 
		    requested_runtypes[ requested_runtype ] += 1
		else : 
		    requested_runtypes[ requested_runtype ] = 1

		requested_libraryprep = sequencing_runs[ run_id ][ project_id ][ 'samples' ][ sample_id ][ 'requested_library_prep' ]
		if requested_libraryprep in requested_librarypreps : 
		    requested_librarypreps[ requested_libraryprep ] +=1
		else : 
		    requested_librarypreps[ requested_libraryprep] = 1
		
		requested_analysis = sequencing_runs[ run_id ][ project_id ][ 'samples' ][ sample_id ][ 'requested_analysis' ]
		if requested_analysis in requested_analyses : 
		    requested_analyses[ requested_analysis ] +=1
		else : 
		    requested_analyses[ requested_analysis ] = 1
		
		sample_type = sequencing_runs[ run_id ][ project_id ][ 'samples' ][ sample_id ][ 'type' ]
		if sample_type in sample_types : 
		    sample_types[ sample_type ] +=1
		else : 
		    sample_types[ sample_type ] = 1
		

		
		lims_runtype = sequencing_runs[ run_id ][ project_id ][ 'samples' ][ sample_id ][ 'lims_runtype' ]
		#pp.pprint(sequencing_runs[run_id][project_id]['samples'][sample_id])
		#print sample_id,lims_runtype
		if lims_runtype in lims_runtypes : 
		    lims_runtypes[ lims_runtype ] +=1
		else : 
		    lims_runtypes[ lims_runtype ] = 1
		
		lims_libraryprep = sequencing_runs[ run_id ][ project_id ][ 'samples' ][ sample_id ][ 'lims_library_prep' ]
		if lims_libraryprep in lims_librarypreps : 
		    lims_librarypreps[ lims_libraryprep ] +=1
		else : 
		    lims_librarypreps[ lims_libraryprep ] = 1
		
		lims_isolation = sequencing_runs[ run_id ][ project_id ][ 'samples' ][ sample_id ][ 'lims_isolation' ]
		if lims_isolation in lims_isolations : 
		    lims_isolations[ lims_isolation ] +=1
		else : 
		    lims_isolations[ lims_isolation ] = 1
	
	    requested_runtype = prepareForPrint(requested_runtypes)
	    requested_libraryprep = prepareForPrint(requested_librarypreps)
	    requested_analysis = prepareForPrint(requested_analyses)
	    sample_type = prepareForPrint(sample_types)
	    lims_runtype = prepareForPrint(lims_runtypes)
	    lims_libraryprep = prepareForPrint(lims_librarypreps)
	    lims_isolation = prepareForPrint(lims_isolations)


	    #determine sequencing costs
	    
	    if len(requested_runtypes.keys()) > 1 or len(lims_runtypes.keys()) > 1:
		errors.append("Multiple runtypes found, can't calculate costs")
	    else:
		first_submission_date = sequencing_runs[ run_id ][ project_id ][ 'first_submission_date' ]
		requested_machine = requested_runtypes.keys()[0].lower()
		requested_machine = requested_machine.split()[0].strip()
		#print run_id,project_id,lims_runtypes.keys()
		lims_machine = lims_runtypes.keys()[0].lower()
		lims_machine = lims_machine.split(':')[1].split()[0].strip()

		if lims_machine == requested_machine and requested_runtypes.keys()[0].lower() in all_costs:
		    for date in sorted( all_costs[ requested_runtypes.keys()[0].lower() ][ 'date_costs'].keys() ):
			if date <= first_submission_date:
		    		billing_date = date
		    		#print date
		#Contingency for if samples were recieved before implementation of cost database
		    if billing_date is None:
			billing_date = sorted( all_costs[ requested_runtypes.keys()[0].lower() ][ 'date_costs'].keys())[0]
			#print billing_date
			errors.append("Could not find a billing date matching the sample recieved date")
			
		    seq_costs = all_costs[ requested_runtypes.keys()[0].lower() ][ 'date_costs' ][ billing_date ]
		    
		else:
		    errors.append("Requested runtype '"+requested_runtypes.keys()[0]+"' doesn't match runtype in LIMS")
		
	    #determine library prep costs
	    if len(requested_librarypreps.keys()) > 1 or len(lims_librarypreps.keys()) > 1:
		errors.append("Multiple library prep types found, please check cost calculation")
	    
	    if lims_librarypreps.keys() :
		for lims_libraryprep in lims_librarypreps:
		    if lims_libraryprep is None : continue
		    lims_libraryprep_ori = lims_libraryprep.split(':')[1].lower().strip()
		    
		    if lims_libraryprep_ori not in requested_librarypreps:
			errors.append("Library prep '"+lims_libraryprep_ori+"' in LIMS different from requested, please check cost calculation")
	    
		    if lims_libraryprep_ori is not None:
		
			if lims_libraryprep_ori in all_costs:
			    ############################FIX LIBRARY PREP COSTS
			    prep_costs += int(all_costs[ lims_libraryprep_ori ][ 'date_costs' ][ billing_date ]) * lims_librarypreps[ lims_libraryprep ]
			else:
			    errors.append("Could not find library prep kit '"+lims_libraryprep_ori+"' in billing database")
		
	    #determine isolation costs
	    if lims_isolations.keys() :
		if len(sample_types.keys()) > 1 or len(lims_isolations.keys()) > 1:
		    errors.append("Multiple sample types found, please check cost calculation")
	    
		for lims_isolation in lims_isolations:
		    if lims_isolation is None : continue
		    lims_isolation_ori = lims_isolation.split(':')[1].lower().strip()
		    if lims_isolation_ori not in sample_types:
			errors.append("Isolation step '"+lims_isolation_ori+"' in LIMS doesn't match sample type")
		    
		    if lims_isolation_ori is not None:
			if lims_isolation_ori in all_costs:
			    iso_costs += int(all_costs[ lims_isolation_ori ][ 'date_costs' ][ billing_date ]) * lims_isolations[ lims_isolation ]
			else:
			    errors.append("Could not find isolation type '" +lims_isolation_ori +"' in billing database")
	    
	    seq_finance.append(
		u"{errors}\t{sequencing_succesful}\t{pool_name}\t{project_name}\t{project_id}\t{open_date}\t{contact_name}\t{contact_email}\t{requested_runtype}\t{lims_runtype}\t{requested_libraryprep}\t{lims_libraryprep}\t{sample_type}\t{lims_isolation}\t{requested_analysis}\t{nr_samples}\t{account}\t{project_budget_number}\t{sequencing_costs}\t{libraryprep_costs}\t{isolation_costs}\t{total_costs}\t{billing_institute}\t{billing_department}\t{billing_postalcode}\t{billing_street}\t{billing_city}\t{billing_country}".format(
		    errors 			= ','.join( set(errors) ),
		    sequencing_succesful 	= sequencing_runs[ run_id ][ project_id ][ 'succesful' ],
		    pool_name			= run_id,
		    project_name		= sequencing_runs[ run_id ][ project_id ][ 'name' ],
		    project_id			= project_id,
		    open_date			= sequencing_runs[ run_id ][ project_id ][ 'open_date' ],
		    contact_name		= sequencing_runs[ run_id ][ project_id ][ 'contact_name' ],
		    contact_email		= sequencing_runs[ run_id ][ project_id ][ 'contact_email' ],
		    requested_runtype		= requested_runtype,
		    lims_runtype		= lims_runtype,
		    requested_libraryprep	= requested_libraryprep,
		    lims_libraryprep 		= lims_libraryprep,
		    sample_type			= sample_type,
		    lims_isolation		= lims_isolation,
		    requested_analysis		= requested_analysis,
		    nr_samples			= sample_count,
		    account			= sequencing_runs[ run_id ][ project_id ][ 'lab_name' ],
		    project_budget_number	= sequencing_runs[ run_id ][ project_id ][ 'budget_nr' ],
		    sequencing_costs		= seq_costs,
		    libraryprep_costs		= prep_costs,
		    isolation_costs		= iso_costs,
		    total_costs			= (int(seq_costs) + int(prep_costs) + int(iso_costs)),
		    billing_institute		= sequencing_runs[ run_id ][ project_id ][ 'institute' ],
		    billing_department		= sequencing_runs[ run_id ][ project_id ][ 'department' ],
		    billing_postalcode		= sequencing_runs[ run_id ][ project_id ][ 'postalcode' ],
		    billing_street		= sequencing_runs[ run_id ][ project_id ][ 'street' ],
		    billing_city		= sequencing_runs[ run_id ][ project_id ][ 'city' ],
		    billing_country		= sequencing_runs[ run_id ][ project_id ][ 'country' ]
		)
	    )
    return seq_finance
	    
	    

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
	mode = getStepWorkflow( options.stepURI + "/details" )
	#print mode
	
	if mode == 'USF : Post Sequencing steps':
	    #getSeqFinance()
	    #seq_finance_table = ['test', 'test']
	    seq_finance_table = getSeqFinance()
	    seq_finance = codecs.open(options.outname, 'w', 'utf-8')
	    seq_finance.write(u"errors\tsequencing_succesful\tpool_name\tproject_name\tproject_id\topen_date\tcontact_name\tcontact_email\trequested_runtype\tlims_runtype\trequested_libraryprep\tlims_libraryprep\tsample_type\tlims_isolation\trequested_analysis\tnr_samples\taccount\tproject_budget_number\tsequencing_costs\tlibraryprep_costs\tisolation_costs\ttotal_costs\tbilling_institute\tbilling_department\tbilling_postalcode\tbilling_street\tbilling_city\tbilling_country\n")
	    seq_finance.write( "\n".join( seq_finance_table ) )
	    seq_finance.close()
	elif mode == 'USF : Post Fingerprinting steps':
	    snpFinanceTable = getSnpFinance()
	    snp_finance = codecs.open(options.outname, 'w', 'utf-8')
	    snp_finance.write(u"errors\tcontainer_name\tproject_name\tid\tsample_name\topen_date\tcontact_name\tcontact_email\tisolated\tsample_type\taccount\tproject_budget_number\tplate_costs\tisolation_costs\tbilling_institute\tbilling_postalcode\tbilling_city\tbilling_country\tbilling_department\tbilling_street\n")
	    snp_finance.write( "\n".join( snpFinanceTable ) )
	    snp_finance.close()
	
if __name__ == "__main__":
	main()