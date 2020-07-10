import sys
import re
import datetime
from genologics.entities import Project

def check( lab_name, project_names ):

    print ("\nYou're about to create the following LIMS run name(s) for account {0}: ".format(lab_name))
    print (", ".join(project_names))
    print ("Is this correct (y)es/(n)o ?")

    yes = set(['yes','y'])
    no = set(['no','n'])
    choice = raw_input().lower()
    if choice in yes:
        choice = True
    elif choice in no:
        choice = False
    else:
       sys.stdout.write("ERROR: Please respond with yes, y, no or n'")

    return choice


def createRunIDs(lims,userid,application, nr):

    researcher = lims.get_researchers(username=userid)[0]
    lab = researcher.lab
    if not researcher:
        sys.exit('ERROR : userid {0} does not exist'.format(userid))

    previous_projects = []
    for project in lims.get_projects():
        project_researcher = project.researcher
        project_lab = project_researcher.lab
        if project_lab.name == lab.name and re.match("^\w+\d+\-\d+",project.name):
            previous_projects.append(project.name)

    new_projects = []
    lab_name_parts = re.sub('[\.\(\)\/]|\sLAB|PROF|DR|PROFESSOR','', lab.name.upper()).split(' ')
    project_base_name = "{0}{1}".format( "".join([x[0] for x in lab_name_parts if x]), lab.id )

    for i in range(len(previous_projects),len(previous_projects)+nr):
        new_projects.append("{0}-{1}".format(project_base_name,i))

    if check(lab.name, new_projects):

        if application == 'Sequencing':
            application = 'USF - Sequencing'
        elif application =='Fingerprinting':
            application = 'USF - SNP genotyping'
        else:
            sys.exit('ERROR: Invalid application "{0}"'.format(application))

        project_ids = []
        for project_name in new_projects:
            project = Project.create(lims,
                name=project_name,
                researcher=researcher,
                open_date=datetime.datetime.today().strftime('%Y-%m-%d'),
                udf = {'Application':application, 'Priority':'Standard'}
            )
            project_ids.append(project.id)
        print ("Created run IDs: {0} for {1} {2} ({3}) in account {4}".format(", ".join(project_ids), researcher.first_name, researcher.last_name, researcher.email, lab.name))

def run(lims, userid, application, nr):

    createRunIDs(lims, userid, application, nr)
