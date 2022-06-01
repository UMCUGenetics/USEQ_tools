from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from csv import DictReader
from genologics.entities import Project
from config import Config
import datetime
import re
import sys


def readCSV(csv):
    csv_info = []
    try:
        csv_reader = DictReader(csv, delimiter=',')
        for row in csv_reader:
            csv_info.append(row)
    except TypeError:
        sys.exit (f"ERROR : Something went wrong reading {csv}, probably the header is missing.")
    return csv_info

def check( csv ,mode ):

    print (f"\nYou're about to do a {mode} action with the following information : ")
    for l in csv.readlines():
        print(l.rstrip())
    print ("Is this correct (y)es/(n)o ?")
    yes = set(['yes','y'])
    no = set(['no','n'])
    choice = input().lower()
    if choice in yes:
        choice = True
    elif choice in no:
        choice = False
    else:
       sys.stdout.write("ERROR: Please respond with yes, y, no or n'")

    return choice


def linkRuns(lims, run_info):
    for run in run_info:
        db_run = session.query(Run).get(run['DB_ID'])
        # sanity checks

        if not db_run: sys.exit (f"ERROR : DB_ID {run['DB_ID']} does not exist, skipping.")
        if db_run.run_id: sys.exit (f"ERROR : DB_ID {run['DB_ID']} already has LIMS_ID {db_run.run_id} assigned to it. ")
        if db_run.user.username != run['USERNAME']: sys.exit(f"ERROR : Owner of DB_ID {run['DB_ID']} does not match USERNAME {run['USERNAME']}.")
        if run['APPLICATION'] not in Config.PROJECT_TYPES.values(): sys.exit(f"ERROR : Invalid APPLICATION, please choose Fingerprinting or Sequencing.")
        if db_run.application != run['APPLICATION']: sys.exit(f"ERROR : APPLICATION does not match preferred application.")
        researchers = lims.get_researchers(username=run['USERNAME'])
        if not researchers: sys.exit(f"ERROR : USERNAME {run['USERNAME']} not found in LIMS.")

        if run['LIMS_ID']:
            project = Project(lims, id=run['LIMS_ID'])
            if not project: sys.exit( f"ERROR : LIMS_ID {run['LIMS_ID']} does not exist.")
            if run['USERNAME'] != project.researcher.username: sys.exit(f"ERROR : LIMS_ID {run['LIMS_ID']} does not belong to {run['USERNAME']}.")
            if lims.get_samples(projectlimsid=project.id): sys.exit(f"ERROR : LIMS_ID {db_run.run_id} already has samples and can not linked.")

            if session.query(Run).filter_by(run_id=project.id).all():
                sys.exit(f"ERROR : LIMS_ID {project.id} was already assigned to another portal DB_ID.")
            else:
                db_run.run_id = project.id
                session.commit()
            print (f"Assigned existing LIMS Run ID {project.id} to portal DB_ID {db_run.id}")
        else:

            lab = researchers[0].lab
            lab_name_parts = re.sub('[\.\(\)\/]|\sLAB|PROF|DR|PROFESSOR','', lab.name.upper()).split(' ')
            project_base_name = "{0}{1}".format( "".join([x[0] for x in lab_name_parts if x]), lab.id )
            new_project_name = None
            nr = 1
            while lims.get_projects(name=f'{project_base_name}-{nr}'):
                nr +=1
            else:
                new_project_name = f'{project_base_name}-{nr}'

            project = Project.create(lims,
                name = new_project_name,
                researcher=researchers[0],
                open_date=datetime.datetime.today().strftime('%Y-%m-%d'),
                udf = {'Application':run['APPLICATION'], 'Priority':'Standard'}
            )
            db_run.run_id = project.id
            session.commit()
            print (f"Assigned new LIMS Run ID {project.id} to portal DB_ID {db_run.id}")


def unlinkRuns(lims, run_info):

    for run in run_info:
        db_run = session.query(Run).get(run['DB_ID'])
        # sanity checks
        if not db_run: sys.exit (f"ERROR : DB_ID {run['DB_ID']} does not exist.")
        if db_run.user.username != run['USERNAME']: sys.exit(f"ERROR : Owner of DB_ID {run['DB_ID']} does not match USERNAME {run['USERNAME']}.")
        researchers = lims.get_researchers(username=run['USERNAME'])
        if not researchers: sys.exit(f"ERROR : USERNAME {run['USERNAME']} not found in LIMS.")

        if db_run.run_id:
            project = Project(lims,id=db_run.run_id)
            if not project: sys.exit(f"ERROR : run_id {db_run.run_id} does not exist in LIMS.")
            if lims.get_samples(projectlimsid=project.id): sys.exit(f"ERROR : run_id {db_run.run_id} already has samples and can not be unlinked.")
            session.delete(db_run)
            session.commit()
            print (f"Unlinked {project.name} ({project.id}) from portal DB_ID {db_run.id}. Please delete LIMS Project {project.name} ({project.id}) manually or reasign it to another portal DB_ID.")

        else:
            session.delete(db_run)
            session.commit()

def run(lims, csv, mode):
    #Set up db connection

    global session
    global User
    global Run
    Base = automap_base()

    # engine, suppose it has two tables 'user' and 'run' set up
    ssl_args = {'ssl_ca': '/etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt'}
    engine = create_engine(Config.PORTAL_DB_URI, connect_args=ssl_args)

    # reflect the tables
    Base.prepare(engine, reflect=True)

    # mapped classes are now created with names by default
    # matching that of the table name.
    User = Base.classes.user
    Run = Base.classes.run

    session = Session(engine)
    if check(csv, mode):
        #reset csv file handle
        csv.seek(0,0)
        csv_info = readCSV(csv)

        if mode=='link':
            linkRuns(lims, csv_info)
        elif mode == 'unlink':
            unlinkRuns(lims, csv_info)
