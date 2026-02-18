"""Module for linking and unlinking runs between portal database and LIMS."""

import datetime
import re
import sys
from csv import DictReader

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import DeclarativeMeta
from genologics.entities import Project, Lab
from genologics.lims import Lims

from config import Config
from modules.useq_ui import query_yes_no
from typing import List, Dict, Any, TextIO, Literal, Optional


def read_csv(csv_file: TextIO) -> List[Dict[str, str]]:
    """
    Read CSV file and return list of dictionaries.

    Args:
        csv_file: File object containing CSV data

    Returns:
        A list of rows, where each row is a dictionary mapping
            column headers to values.

    Raises:
        SystemExit: If CSV cannot be read or header is missing
    """
    csv_info = []
    try:
        csv_reader = DictReader(csv_file, delimiter=',')
        for row in csv_reader:
            csv_info.append(row)
    except TypeError:
        sys.exit(f"ERROR: Something went wrong reading {csv_file}, "
                 f"probably the header is missing.")
    return csv_info


def _validate_db_run(db_run: Any, run: Dict[str, Any], session: Session):
    """
    Validate database run entry.

    Args:
        db_run: Database run object
        run: Dictionary with run information from CSV
        session: SQLAlchemy session

    Raises:
        SystemExit: If validation fails
    """
    if not db_run:
        sys.exit(f"ERROR: DB_ID {run['DB_ID']} does not exist.")

    if db_run.run_id:
        sys.exit(f"ERROR: DB_ID {run['DB_ID']} already has LIMS_ID "
                 f"{db_run.run_id} assigned to it.")

    if db_run.user.username != run['USERNAME']:
        sys.exit(f"ERROR: Owner of DB_ID {run['DB_ID']} does not match "
                 f"USERNAME {run['USERNAME']}.")


def _validate_lims_project(lims: Lims, project: Project, run: Dict[str, Any], session: Session, Run: DeclarativeMeta):
    """Validate LIMS project.

    Args:
        lims : LIMS instance
        project: LIMS Project object
        run: Dictionary with run information from CSV
        session: SQLAlchemy session
        Run: SQLAlchemy Run class

    Raises:
        SystemExit: If validation fails
    """
    if not project:
        sys.exit(f"ERROR: LIMS_ID {run['LIMS_ID']} does not exist.")

    if run['USERNAME'] != project.researcher.username:
        sys.exit(f"ERROR: LIMS_ID {run['LIMS_ID']} does not belong to "
                 f"{run['USERNAME']}.")

    if lims.get_samples(projectlimsid=project.id):
        sys.exit(f"ERROR: LIMS_ID {project.id} already has samples and "
                 f"cannot be linked.")

    if session.query(Run).filter_by(run_id=project.id).all():
        sys.exit(f"ERROR: LIMS_ID {project.id} was already assigned to "
                 f"another portal DB_ID.")


def _generate_project_name(lims: Lims, lab: Lab) -> str:
    """Generate unique project name based on lab information.

    Args:
        lims: LIMS instance
        lab: Lab object

    Returns:
        Unique project name string
    """
    # Clean lab name and create base name
    lab_name_parts = re.sub(
        r'[\.\(\)\/]|\sLAB|PROF|DR|PROFESSOR',
        '',
        lab.name.upper()
    ).split(' ')

    project_base_name = "{0}{1}".format(
        "".join([x[0] for x in lab_name_parts if x]),
        lab.id
    )

    # Find next available number
    nr = 1
    while lims.get_projects(name=f'{project_base_name}-{nr}'):
        nr += 1

    return f'{project_base_name}-{nr}'


def link_runs(lims: Lims, run_info: List[Dict[str, Any]], session: Session, Run: DeclarativeMeta):
    """Link portal runs to LIMS projects.

    Args:
        lims: LIMS instance
        run_info: List of dictionaries with run information
        session: SQLAlchemy session
        Run: SQLAlchemy Run class
    """
    for run in run_info:
        db_run = session.query(Run).get(run['DB_ID'])

        # Validate database run
        _validate_db_run(db_run, run, session)

        # Validate researcher exists in LIMS
        researchers = lims.get_researchers(username=run['USERNAME'])
        if not researchers:
            sys.exit(f"ERROR: USERNAME {run['USERNAME']} not found in LIMS.")

        if run['LIMS_ID']:
            # Link to existing project
            project = Project(lims, id=run['LIMS_ID'])
            _validate_lims_project(lims, project, run, session, Run)

            db_run.run_id = project.id
            session.commit()
            print(f"Assigned existing LIMS Run ID {project.id} to "
                  f"portal DB_ID {db_run.id}")
        else:
            # Create new project
            lab = researchers[0].lab
            new_project_name = _generate_project_name(lims, lab)

            project = Project.create(
                lims,
                name=new_project_name,
                researcher=researchers[0],
                open_date=datetime.datetime.today().strftime('%Y-%m-%d'),
                udf={'Priority': 'Standard'}
            )

            db_run.run_id = project.id
            session.commit()
            print(f"Assigned new LIMS Run ID {project.id} to "
                  f"portal DB_ID {db_run.id}")


def unlink_runs(lims: Lims, run_info: List[Dict[str, Any]], session: Session, Run: DeclarativeMeta):
    """Unlink portal runs from LIMS projects.

    Args:
        lims: LIMS connection object
        run_info: List of dictionaries with run information
        session: SQLAlchemy session
        Run: SQLAlchemy Run class
    """
    for run in run_info:
        db_run = session.query(Run).get(run['DB_ID'])

        # Sanity checks
        if not db_run:
            sys.exit(f"ERROR: DB_ID {run['DB_ID']} does not exist.")

        if db_run.user.username != run['USERNAME']:
            sys.exit(f"ERROR: Owner of DB_ID {run['DB_ID']} does not match "
                     f"USERNAME {run['USERNAME']}.")

        researchers = lims.get_researchers(username=run['USERNAME'])
        if not researchers:
            sys.exit(f"ERROR: USERNAME {run['USERNAME']} not found in LIMS.")

        if db_run.run_id:
            project = Project(lims, id=db_run.run_id)
            if not project:
                sys.exit(f"ERROR: run_id {db_run.run_id} does not exist in LIMS.")

            if lims.get_samples(projectlimsid=project.id):
                sys.exit(f"ERROR: run_id {db_run.run_id} already has samples "
                         f"and cannot be unlinked.")

            session.delete(db_run)
            session.commit()
            print(f"Unlinked {project.name} ({project.id}) from portal "
                  f"DB_ID {db_run.id}. Please delete LIMS Project "
                  f"{project.name} ({project.id}) manually or reassign it "
                  f"to another portal DB_ID.")
        else:
            session.delete(db_run)
            session.commit()


def run(lims: Lims, csv_file: TextIO, mode: Literal['link', 'unlink']):
    """Main function to link or unlink runs.

    Args:
        lims: LIMS instance
        csv_file: File object containing CSV data
        mode: Either 'link' or 'unlink'
    """
    # Set up database connection
    Base = automap_base()
    ssl_args = {'ssl_ca': '/etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt'}
    engine = create_engine(Config.PORTAL_DB_URI, connect_args=ssl_args)

    # Reflect the tables
    Base.prepare(engine, reflect=True)

    # Get mapped classes
    User = Base.classes.user
    Run = Base.classes.run
    session = Session(engine)

    # Display preview and confirm
    print(f"\nYou're about to do a {mode} action with the following information:")
    for line in csv_file.readlines():
        print(line.rstrip())

    if query_yes_no("Is this correct?"):
        # Reset CSV file handle
        csv_file.seek(0, 0)
        csv_info = read_csv(csv_file)

        if mode == 'link':
            link_runs(lims, csv_info, session, Run)
        elif mode == 'unlink':
            unlink_runs(lims, csv_info, session, Run)
