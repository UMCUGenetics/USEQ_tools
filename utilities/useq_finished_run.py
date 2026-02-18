"""Module for managing project workflow transitions and sequencing stats updates."""

import sys
from typing import Tuple, Any

from genologics.entities import Project,Step,Queue,StepActions,StepDetails,Stage
from genologics.lims import Lims
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

from config import Config


def create_db_session() -> Tuple[Session, Any, Any, Any]:
    """Create and configure a database session with automap models.

    This function establishes a connection to the portal database using
    SQLAlchemy's automap feature to reflect existing database tables as
    Python classes.

    Returns:
        A tuple containing:
            - session: SQLAlchemy Session object for database operations
            - Run: Automap class representing the 'run' table
            - IlluminaSequencingStats: Automap class for Illumina stats table
            - NanoporeSequencingStats: Automap class for Nanopore stats table

    """
    Base = automap_base()
    ssl_args = {'ssl_ca': Config.SSL_CERT}
    engine = create_engine(
        Config.PORTAL_DB_URI,
        connect_args=ssl_args,
        pool_pre_ping=True,
        pool_recycle=21600
    )
    Base.prepare(engine, reflect=True)

    Run = Base.classes.run
    IlluminaSequencingStats = Base.classes.illumina_sequencing_stats
    NanoporeSequencingStats = Base.classes.nanopore_sequencing_stats
    session = Session(engine)

    return (session, Run, IlluminaSequencingStats, NanoporeSequencingStats)


def move_project(lims: Lims, project_id: str, flowcell_id: str, successful: bool) -> None:
    """Move project through workflow stages and update sequencing statistics.

    This function processes artifacts in the "Process Raw Data" queue for a
    specific project, advances them through workflow stages, and updates the
    sequencing success status in both LIMS and the portal database.

    Args:
        lims (Lims): LIMS instance.
        project_id (str): LIMS project ID.
        flowcell_id (str): The flowcell identifier associated with the sequencing run.
        successful (bool): Boolean indicating whether the sequencing was successful.

    Raises:
        SystemExit: If the project ID is not found in LIMS or if no stats exist in the portal database for the given flowcell.

    """
    try:
        project = Project(lims, id=project_id)
        project_name = project.name
    except Exception:
        sys.exit(f"Error: Project ID {project_id} not found in LIMS!")

    # Get workflow stage configurations
    processing_stage_nrs = Config.WORKFLOW_STEPS['SEQUENCING']['steps'][
        'POST SEQUENCING']['stage_nrs']['USEQ - Post Sequencing']
    analysis_stage_nrs = Config.WORKFLOW_STEPS['SEQUENCING']['steps'][
        'POST SEQUENCING']['stage_nrs']['USEQ - Analysis']
    billing_stage_nrs = Config.WORKFLOW_STEPS['SEQUENCING']['steps'][
        'POST SEQUENCING']['stage_nrs']['USEQ - Ready for billing']

    # Create stage objects
    workflow_id, stage_id = processing_stage_nrs.split(':')
    processing_stage = Stage(
        lims,
        uri=f"{Config.LIMS_URI}/api/v2/configuration/workflows/"
            f"{workflow_id}/stages/{stage_id}"
    )

    workflow_id, stage_id = analysis_stage_nrs.split(':')
    analysis_stage = Stage(
        lims,
        uri=f"{Config.LIMS_URI}/api/v2/configuration/workflows/"
            f"{workflow_id}/stages/{stage_id}"
    )

    workflow_id, stage_id = billing_stage_nrs.split(':')
    billing_stage = Stage(
        lims,
        uri=f"{Config.LIMS_URI}/api/v2/configuration/workflows/"
            f"{workflow_id}/stages/{stage_id}"
    )

    processing_step_uri = processing_stage.step.uri
    analysis_step_uri = analysis_stage.step.uri
    billing_step_uri = billing_stage.step.uri

    # Process artifacts in the queue
    queue = Queue(lims, id=processing_stage.step.id)

    for artifact in queue.artifacts:
        if artifact.samples[0].project.id == project_id:
            step_uri = None

            # Find the queued step URI
            for sas in artifact.workflow_stages_and_statuses:
                if sas[1] == 'QUEUED' and sas[2] == 'USEQ - Process Raw Data':
                    step_uri = sas[0].step.uri

            # Create step XML
            xml = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
                <tmp:step-creation xmlns:tmp="http://genologics.com/ri/step">
                    <configuration uri="{step_uri}"/>
                    <container-type>Tube</container-type>
                    <inputs>
                        <input uri="{artifact.uri}" replicates="1"/>
                    </inputs>
                </tmp:step-creation>
            '''

            # Create and advance the step
            output = lims.post(uri=lims.get_uri('steps'), data=xml)
            step_details_uri = output.find('details').get('uri')
            step_details = StepDetails(lims, uri=step_details_uri)

            actions = []
            for io_map in step_details.input_output_maps:
                if io_map[1]['output-generation-type'] == 'PerInput':
                    output_artifact = io_map[1]['uri']
                    output_artifact.udf['Sequencing Succesful'] = successful
                    output_artifact.put()
                    actions.append({
                        'action': 'nextstep',
                        'artifact': output_artifact,
                        'step-uri': analysis_step_uri
                    })

            # Advance step and set next actions
            step_action_uri = output.find('actions').get('uri')
            step_actions = StepActions(lims, uri=step_action_uri)
            step = step_actions.step
            step.advance()
            step_actions.set_next_actions(actions)
            step_actions.put()
            step.advance()

    # Update portal database
    session, Run, IlluminaSequencingStats, NanoporeSequencingStats = (
        create_db_session()
    )
    portal_run = session.query(Run).filter_by(run_id=project_id).first()

    if portal_run.platform == 'Oxford Nanopore':
        stats = session.query(NanoporeSequencingStats).filter_by(flowcell_id=flowcell_id).first()
        if not stats:
            sys.exit(f"Error: No existing stats found in portal for {project_id}")
        stats.succesful = successful
        session.commit()
    else:
        stats = session.query(IlluminaSequencingStats).filter_by(flowcell_id=flowcell_id).first()
        if not stats:
            sys.exit(f"Error: No existing stats found in portal for {project_id}")
        stats.succesful = successful
        session.commit()


def run(lims: Lims, project_id: str, flowcell_id: str, successful: bool) -> None:
    """Execute the project move process.

    Args:
        lims (Lims): LIMS instance.
        project_id (str): LIMS project ID.
        flowcell_id (str): The flowcell identifier associated with the sequencing run.
        successful (bool): Boolean indicating whether the sequencing was successful.

    Returns:
        None
    """
    move_project(lims, project_id, flowcell_id, successful)
