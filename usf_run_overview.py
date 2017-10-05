import glsapiutil
import xml.dom.minidom
import smtplib
import urllib
import os
import re
import sys
from usf_run_overview_config import MAPPING_DIRS, RAW_DATA_DIRS, BACKUP_DIRS,SEQUENCING_PROCESSES ,USERNAME, PASSWORD, BASEURI
import time
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

def updateProjectsFromLIMS( ):

    pl_XML = api.getResourceByURI( BASE_URI + 'projects' )
    pl_DOM = parseString( pl_XML )
    nodes = None

    project_nodes = pl_DOM.getElementsByTagName( "project" )
    for project_node in project_nodes:
        project = { 'name': '','id':'','owner':'','email':'', 'analysis': '','process':'','open_date':'' ,'conversion_date':'' ,'processed_date':'', 'backup_date':'', 'raw_data':'', 'mapped_data':[], 'backup_data':[]}


        project['name'] = project_node.getElementsByTagName( "name" )[0].firstChild.data
        p_id = project_node.getAttribute( "limsid" )
        project['id'] = p_id
        project['analysis'] = getAnalysis( p_id )

        p_XML = api.getResourceByURI( BASE_URI + 'projects/' + project['id'] )
        p_DOM = parseString( p_XML )

        project['open_date'] = p_DOM.getElementsByTagName( "open-date" )[0].firstChild.data

        #If project application is SNP array, skip it
        if ( api.getUDF( p_DOM, "Application" ) == 'SNP Array' ):
            continue

        #Get project owner info, from cache if possible
        r_URI = p_DOM.getElementsByTagName( "researcher" )[0].getAttribute( "uri" )
        r_URI = r_URI.replace( ":8443" , "" )
        r_DOM = getObjectDOM( r_URI )
        r_first_name = r_DOM.getElementsByTagName( "first-name" )[0].firstChild.data
        r_last_name = r_DOM.getElementsByTagName( "last-name" )[0].firstChild.data
        r_email = ''

        if ( r_DOM.getElementsByTagName( "email" ) ):
            r_email = r_DOM.getElementsByTagName( "email" )[0].firstChild.data

        project['owner'] = r_first_name + ' ' + r_last_name
        project['email'] = r_email

        #Get process id from appropriate process (Nextseq, miseq or hiseq)
        pr_XML = None
        pr_id = None
        p_process = None

        for seq_process in SEQUENCING_PROCESSES:
            pr_XML = api.getResourceByURI( BASE_URI + 'processes/?projectname=' + urllib.quote(project['name']) + '&type='+ urllib.quote( SEQUENCING_PROCESSES[seq_process] ) )
            pr_DOM = parseString( pr_XML )
            if ( len ( pr_DOM.getElementsByTagName( "process" ) ) > 0 ):
                pr_id = pr_DOM.getElementsByTagName( "process" )[0].getAttribute( "limsid" )
                p_process = SEQUENCING_PROCESSES[seq_process]
                project['process'] = p_process
                break


        #If no process information could be found for project , skip it
        if (not pr_id):
            continue



        #pr_id = pr_DOM.getElementsByTagName( "process" )[0].getAttribute( "limsid" )
        #overwrite previous process xml
        pr_XML = api.getResourceByURI( BASE_URI + 'processes/' + pr_id )
        pr_DOM = parseString( pr_XML )
        flow_cell = api.getUDF( pr_DOM, "Flow Cell ID")
        if ( len( flow_cell ) > 0):
            if flow_cell not in PROJECT_INFO:
                PROJECT_INFO[flow_cell] = project
        #print flow_cell, project
        #else:
        #print "No flowcell found for project limsid "+ project['id']

def updateProjectsFromRaw():

    for platform in RAW_DATA_DIRS:

        for sequencer_dir in RAW_DATA_DIRS[platform]:

            for item in os.listdir(sequencer_dir):
                if os.path.isdir(os.path.join(sequencer_dir, item)):
                    run_dir = os.path.join(sequencer_dir, item)
                    flow_cell = run_dir.split("_")[-1]
                    flow_cell = re.sub("^A", "",flow_cell)
                    flow_cell = re.sub("^B", "",flow_cell)
                    flow_cell = re.sub("\d+-", "",flow_cell)
                    flow_cell = re.sub("REDO", "",flow_cell)
                    run_parameters_file = None
                    project_name = ''
                    conversion_date = ''

                    if(os.path.isfile( run_dir + '/RunParameters.xml' ) ):
                        run_parameters_file = run_dir + '/RunParameters.xml'
                    elif(os.path.isfile( run_dir + '/runParameters.xml') ):
                        run_parameters_file = run_dir + '/runParameters.xml'

                    if (run_parameters_file):
                        conversion_date = time.strftime("%Y-%m-%d", time.gmtime( os.path.getctime( run_parameters_file ) ) )

                        f = open(run_parameters_file, 'r')
                        run_parameters_XML = f.read()
                        run_parameters_DOM = parseString( run_parameters_XML )

                        try:
                            project_name =  run_parameters_DOM.getElementsByTagName( "ExperimentName" )[0].firstChild.data
                            project_name = re.sub("REDO", "",project_name)
                            project_name = re.sub("_", "",project_name)
                        except:
                            continue
                        f.close()
                    else:
                        continue

                    if(flow_cell in PROJECT_INFO):
                        PROJECT_INFO[flow_cell]['raw_data'] = run_dir
                        PROJECT_INFO[flow_cell]['conversion_date'] = conversion_date

                    else:

                        p_XML = api.getResourceByURI( BASE_URI + 'projects/?name=' + project_name)
                        p_DOM = parseString( p_XML )
                        p_nodes = p_DOM.getElementsByTagName( 'project' )


                        if ( len(p_nodes) > 0 ):
                            print "By name {0}".format(project_name)
                            PROJECT_INFO[flow_cell] = { 'name': project_name,'id':'','owner':'','email':'','process':'','analysis': '', 'open_date':'' ,'conversion_date':conversion_date ,'processed_date':'', 'backup_date':'','raw_data':run_dir, 'mapped_data':[], 'backup_data':[]}
                            p_id = p_nodes[0].getAttribute('limsid')
                            PROJECT_INFO[flow_cell]['id'] = p_id


                            p_XML = api.getResourceByURI( BASE_URI + 'projects/' + p_id )
                            p_DOM = parseString( p_XML )

                            PROJECT_INFO[flow_cell]['open_date'] = p_DOM.getElementsByTagName( "open-date" )[0].firstChild.data

                            r_URI = p_DOM.getElementsByTagName( "researcher" )[0].getAttribute( "uri" )
                            r_URI = r_URI.replace( ":8443" , "" )
                            r_DOM = getObjectDOM( r_URI )
                            r_first_name = r_DOM.getElementsByTagName( "first-name" )[0].firstChild.data
                            r_last_name = r_DOM.getElementsByTagName( "last-name" )[0].firstChild.data
                            r_email = ''

                            if ( r_DOM.getElementsByTagName( "email" ) ):
                                r_email = r_DOM.getElementsByTagName( "email" )[0].firstChild.data

                            PROJECT_INFO[flow_cell]['owner'] = r_first_name + ' ' + r_last_name
                            PROJECT_INFO[flow_cell]['email'] = r_email

                            PROJECT_INFO[flow_cell]['analysis'] = getAnalysis( p_id )
                            PROJECT_INFO[flow_cell]['process'] = SEQUENCING_PROCESSES[platform]
                        else:

                            print "By id {0}".format(project_name)
                            try:
                                p_XML = api.getResourceByURI( BASE_URI + 'projects/' + project_name )
                                p_DOM = parseString( p_XML )
                                p_name = p_DOM.getElementsByTagName("name")[0].firstChild.data
                            except:
                                continue

                            PROJECT_INFO[flow_cell] = { 'name': p_name,'id':project_name,'owner':'','email':'','process':'','analysis': '', 'open_date':'' ,'conversion_date':conversion_date ,'processed_date':'', 'backup_date':'','raw_data':run_dir, 'mapped_data':[], 'backup_data':[]}
                            PROJECT_INFO[flow_cell]['open_date'] = p_DOM.getElementsByTagName( "open-date" )[0].firstChild.data

                            r_URI = p_DOM.getElementsByTagName( "researcher" )[0].getAttribute( "uri" )
                            r_URI = r_URI.replace( ":8443" , "" )
                            r_DOM = getObjectDOM( r_URI )
                            r_first_name = r_DOM.getElementsByTagName( "first-name" )[0].firstChild.data
                            r_last_name = r_DOM.getElementsByTagName( "last-name" )[0].firstChild.data
                            r_email = ''

                            if ( r_DOM.getElementsByTagName( "email" ) ):
                                r_email = r_DOM.getElementsByTagName( "email" )[0].firstChild.data

                            PROJECT_INFO[flow_cell]['owner'] = r_first_name + ' ' + r_last_name
                            PROJECT_INFO[flow_cell]['email'] = r_email

                            PROJECT_INFO[flow_cell]['analysis'] = getAnalysis( project_name )
                            PROJECT_INFO[flow_cell]['process'] = SEQUENCING_PROCESSES[platform]

def updateProjectsFromBackup():
    for platform in BACKUP_DIRS:
        for sequencer_dir in BACKUP_DIRS[platform]:
            for item in os.listdir(sequencer_dir):
                run_dir = sequencer_dir + '/' + item
                settings_file = ''
                settings_date = ''

                if( os.path.isdir(run_dir)):
                    (settings_date, settings_file) = getMappingSettings(run_dir)
                else:
                    continue

                f = None
                if settings_file:
                    f = open(settings_file, 'r')
                else:
                    continue

                flow_cells = {}
                for line in f.readlines():
                    line = line.rstrip()
                    if ".fastq.gz" in line:
                        (flow_cell,project_name) = getRunInfoFromFastq(line)

                        if flow_cell not in flow_cells and flow_cell != '':
                            flow_cells[flow_cell] = project_name

                for flow_cell in flow_cells:
                    if flow_cell in PROJECT_INFO:
                        #flow_cells.append(flow_cell)
                        PROJECT_INFO[flow_cell]['backup_data'].append(run_dir)
                        PROJECT_INFO[flow_cell]['backup_date'] = settings_date
                    else:
                        project_name = flow_cells[flow_cell]

                        PROJECT_INFO[flow_cell] = { 'name': project_name,'id':'','owner':'','email':'','process':'','analysis': '', 'open_date':'' ,'conversion_date':'' ,'processed_date':'', 'backup_date':settings_date,'raw_data':'', 'mapped_data':[], 'backup_data':[]}
                        PROJECT_INFO[flow_cell]['backup_data'].append(run_dir)


                        p_XML = api.getResourceByURI( BASE_URI + 'projects/?name=' + project_name)
                        p_DOM = parseString( p_XML )
                        p_nodes = p_DOM.getElementsByTagName( 'project' )
                        if ( len(p_nodes) > 0 ):
                            p_id = p_nodes[0].getAttribute('limsid')
                            PROJECT_INFO[flow_cell]['id'] = p_id
                            p_XML = api.getResourceByURI( BASE_URI + 'projects/' + p_id )
                            p_DOM = parseString( p_XML )

                            PROJECT_INFO[flow_cell]['open_date'] = p_DOM.getElementsByTagName( "open-date" )[0].firstChild.data

                            r_URI = p_DOM.getElementsByTagName( "researcher" )[0].getAttribute( "uri" )
                            r_URI = r_URI.replace( ":8443" , "" )
                            r_DOM = getObjectDOM( r_URI )
                            r_first_name = r_DOM.getElementsByTagName( "first-name" )[0].firstChild.data
                            r_last_name = r_DOM.getElementsByTagName( "last-name" )[0].firstChild.data
                            r_email = ''
                            if ( r_DOM.getElementsByTagName( "email" ) ):
                                r_email = r_DOM.getElementsByTagName( "email" )[0].firstChild.data

                            PROJECT_INFO[flow_cell]['owner'] = r_first_name + ' ' + r_last_name
                            PROJECT_INFO[flow_cell]['email'] = r_email

                            PROJECT_INFO[flow_cell]['analysis'] = getAnalysis( p_id )
                            PROJECT_INFO[flow_cell]['process'] = SEQUENCING_PROCESSES[platform]

def updateProjectsFromMapped():
    for platform in MAPPING_DIRS:
        for sequencer_dir in MAPPING_DIRS[platform]:
            for item in os.listdir(sequencer_dir):
                run_dir = sequencer_dir + '/' + item
                settings_file = ''
                settings_date = ''

                if( os.path.isdir(run_dir)):
                    (settings_date, settings_file) = getMappingSettings(run_dir)
                else:
                    continue

                f = None
                if settings_file:
                    f = open(settings_file, 'r')
                else:
                    continue

                flow_cells = {}
                for line in f.readlines():
                    line = line.rstrip()
                    if ".fastq.gz" in line:
                        (flow_cell,project_name) = getRunInfoFromFastq(line)

                        if flow_cell not in flow_cells and flow_cell != '':
                            flow_cells[flow_cell] = project_name

                for flow_cell in flow_cells:
                    if flow_cell in PROJECT_INFO:
                        #flow_cells.append(flow_cell)
                        PROJECT_INFO[flow_cell]['mapped_data'].append(run_dir)
                        PROJECT_INFO[flow_cell]['processed_date'] = settings_date
                    else:
                        project_name = flow_cells[flow_cell]

                        PROJECT_INFO[flow_cell] = { 'name': project_name,'id':'','owner':'','email':'','analysis':'','process':'', 'open_date':'' ,'conversion_date':'' ,'processed_date':settings_date, 'backup_date':'','raw_data':'', 'mapped_data':[], 'backup_data':[]}
                        PROJECT_INFO[flow_cell]['mapped_data'].append(run_dir)


                        p_XML = api.getResourceByURI( BASE_URI + 'projects/?name=' + project_name)
                        p_DOM = parseString( p_XML )
                        p_nodes = p_DOM.getElementsByTagName( 'project' )

if ( len(p_nodes) > 0 ):
p_id = p_nodes[0].getAttribute('limsid')
PROJECT_INFO[flow_cell]['id'] = p_id
p_XML = api.getResourceByURI( BASE_URI + 'projects/' + p_id )
p_DOM = parseString( p_XML )

PROJECT_INFO[flow_cell]['open_date'] = p_DOM.getElementsByTagName( "open-date" )[0].firstChild.data

r_URI = p_DOM.getElementsByTagName( "researcher" )[0].getAttribute( "uri" )
r_URI = r_URI.replace( ":8443" , "" )
r_DOM = getObjectDOM( r_URI )
r_first_name = r_DOM.getElementsByTagName( "first-name" )[0].firstChild.data
r_last_name = r_DOM.getElementsByTagName( "last-name" )[0].firstChild.data
r_email = ''
if ( r_DOM.getElementsByTagName( "email" ) ):
r_email = r_DOM.getElementsByTagName( "email" )[0].firstChild.data

PROJECT_INFO[flow_cell]['owner'] = r_first_name + ' ' + r_last_name
PROJECT_INFO[flow_cell]['email'] = r_email
#PROJECT_INFO[flow_cell]['analysis'] = getAnalysis( p_id )
PROJECT_INFO[flow_cell]['process'] = SEQUENCING_PROCESSES[platform]
def getAnalysis(p_id):

analysis = ''
#print "Process ID\t"+p_id

s_XML = api.getResourceByURI( BASE_URI + 'samples/?projectlimsid=' + p_id )
s_DOM = parseString( s_XML )

try:
s_id = s_DOM.getElementsByTagName( "sample" )[0].getAttribute( 'limsid' )
s_XML = api.getResourceByURI( BASE_URI + 'samples/' + s_id )
s_DOM = parseString( s_XML )
analysis = api.getUDF( s_DOM, "Analysis")

except:
return analysis

#print "Analysis\t"+analysis
return analysis


def getRunInfoFromFastq(fastq):

project_name = ''
flow_cell = ''
match_obj = re.match(r'.*_(.+)/Data/Intensities/BaseCalls/(.+?)/.*' , fastq)
match_obj2 = re.match(r'.*_(.+)/Unaligned/Project_(.+)/Sample' , fastq)
match_obj3 = re.match(r'.*_(.+XX)/(.+?)/.*',fastq)
if match_obj:
flow_cell = match_obj.group(1)
project_name = match_obj.group(2)
#print '1'

elif match_obj2:
flow_cell = match_obj2.group(1)
project_name = match_obj2.group(2)
#print '2'
elif match_obj3:
flow_cell = match_obj3.group(1)
project_name = match_obj3.group(2)
#print '3'

#print project_name, flow_cell
project_name = re.sub("/", "",project_name)
project_name = re.sub("REDO", "",project_name)
project_name = re.sub("_", "",project_name)

flow_cell = re.sub("^A", "",flow_cell)
flow_cell = re.sub("^B", "",flow_cell)
flow_cell = re.sub("\d+-", "",flow_cell)
#flow_cell = re.sub("REDO", "",flow_cell)
if project_name.lower() != 'data':
return (flow_cell,project_name)
else:
return (flow_cell,'')




def getMappingSettings(run_dir):
settings_file = ''
settings_date = ''
if ( os.path.isfile(run_dir + '/settings.config') ):
settings_file = run_dir + '/settings.config'
settings_date = time.strftime("%Y-%m-%d", time.gmtime( os.path.getctime( settings_file ) ) )
#return [settings_date, settings_file]
else:
for item in os.listdir(run_dir):
if ( os.path.isfile ( run_dir + '/' + item ) and item.startswith('settings') and item.endswith('txt') ):
settings_file = run_dir + '/' + item
settings_date = time.strftime("%Y-%m-%d", time.gmtime( os.path.getctime( settings_file ) ) )

return [settings_date ,settings_file]

def main():

global api

setupGlobalsFromURI( API_URI )
api = glsapiutil.glsapiutil()
api.setHostname( HOSTNAME )
api.setVersion( VERSION )
api.setup( API_USER, API_PW )
reload(sys)
sys.setdefaultencoding('utf8')
#current_date = datetime.fromtimestamp( time.localtime() )
#print current_date
## at this point, we have the parameters the EPP plugin passed, and we have network plumbing
## so let's get this show on the road!
updateProjectsFromLIMS()
updateProjectsFromRaw()
updateProjectsFromBackup()
updateProjectsFromMapped()

print "Process\tFlowcell\tLIMS Name\tLIMS ID\tOwner\tOwner Email\tAnalysis\tOpen Date\tConversion Date\tProcessed Date\tBackup Date\tDays after Open\tDays after Conversion\tDays after Processing\tRaw data loc\tMapped data loc\tBackup data loc"
for flow_cell in PROJECT_INFO:
if (not PROJECT_INFO[flow_cell]['id']):
continue

days_after_open = 'NA'
days_after_conversion = 'NA'
days_after_processing = 'NA'

current_date = time.strftime("%Y-%m-%d", time.localtime())
current_date = time.strptime( current_date , "%Y-%m-%d")
current_date = datetime.fromtimestamp( time.mktime(current_date) )

if ( PROJECT_INFO[flow_cell]['open_date'] ):
dao = time.strptime( PROJECT_INFO[flow_cell]['open_date'] , "%Y-%m-%d")
dao = datetime.fromtimestamp( time.mktime(dao) )
days_after_open = ( current_date - dao ).days

if ( PROJECT_INFO[flow_cell]['conversion_date'] ):
dac = time.strptime( PROJECT_INFO[flow_cell]['conversion_date'] , "%Y-%m-%d")
dac = datetime.fromtimestamp(time.mktime(dac))
days_after_conversion = ( current_date - dac ).days

if ( PROJECT_INFO[flow_cell]['processed_date'] ):
dap = time.strptime( PROJECT_INFO[flow_cell]['processed_date'] , "%Y-%m-%d")
dap = datetime.fromtimestamp(time.mktime(dap))
days_after_processing = ( current_date - dap ).days

print PROJECT_INFO[flow_cell]['process'] + "\t" + flow_cell + "\t" + "\t".join([PROJECT_INFO[flow_cell]['name'],
PROJECT_INFO[flow_cell]['id'],
PROJECT_INFO[flow_cell]['owner'],
PROJECT_INFO[flow_cell]['email'],
PROJECT_INFO[flow_cell]['analysis'],
PROJECT_INFO[flow_cell]['open_date'],
PROJECT_INFO[flow_cell]['conversion_date'],
PROJECT_INFO[flow_cell]['processed_date'],
PROJECT_INFO[flow_cell]['backup_date'],
str(days_after_open),
str(days_after_conversion),
str(days_after_processing),
PROJECT_INFO[flow_cell]['raw_data'],
",".join(PROJECT_INFO[flow_cell]['mapped_data']),
",".join(PROJECT_INFO[flow_cell]['backup_data'])])

if __name__ == "__main__":
main()
