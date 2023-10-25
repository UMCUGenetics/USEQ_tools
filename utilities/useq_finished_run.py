from genologics.entities import Project,Step, Queue,StepActions,StepDetails, Stage
from config import Config
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
import sys

def createDBSession():

    Base = automap_base()
    ssl_args = {'ssl_ca': Config.SSL_CERT}
    engine = create_engine(Config.PORTAL_DB_URI, connect_args=ssl_args, pool_pre_ping=True, pool_recycle=21600)

    Base.prepare(engine, reflect=True)
    Run = Base.classes.run
    IlluminaSequencingStats = Base.classes.illumina_sequencing_stats
    NanoporeSequencingStats = Base.classes.nanopore_sequencing_stats
    session = Session(engine)

    return (session,Run, IlluminaSequencingStats,NanoporeSequencingStats)


def move_project(lims, project_id, fid, succesful):

    try:
        project = Project(lims, id=project_id)
        project_name = project.name
    except:
        sys.exit(f"Error : Project ID {project_id} not found in LIMS!")

    actions = []

    processing_stage_nrs = Config.WORKFLOW_STEPS['SEQUENCING']['steps']['POST SEQUENCING']['stage_nrs']['USEQ - Post Sequencing']
    analysis_stage_nrs = Config.WORKFLOW_STEPS['SEQUENCING']['steps']['POST SEQUENCING']['stage_nrs']['USEQ - Analysis']
    billing_stage_nrs = Config.WORKFLOW_STEPS['SEQUENCING']['steps']['POST SEQUENCING']['stage_nrs']['USEQ - Ready for billing']

    processing_stage = Stage(lims,uri=f"{Config.LIMS_URI}/api/v2/configuration/workflows/{processing_stage_nrs.split(':')[0]}/stages/{processing_stage_nrs.split(':')[1]}")
    analysis_stage = Stage(lims,uri=f"{Config.LIMS_URI}/api/v2/configuration/workflows/{analysis_stage_nrs.split(':')[0]}/stages/{analysis_stage_nrs.split(':')[1]}")
    billing_stage = Stage(lims,uri=f"{Config.LIMS_URI}/api/v2/configuration/workflows/{billing_stage_nrs.split(':')[0]}/stages/{billing_stage_nrs.split(':')[1]}")

    processing_step_uri = processing_stage.step.uri
    analysis_step_uri = analysis_stage.step.uri
    billing_step_uri = billing_stage.step.uri

    queue = Queue(lims, id=processing_stage.step.id) #Process raw data queue

    for artifact in queue.artifacts:
        if artifact.samples[0].project.id == project_id:
            step_uri = None
            for sas in artifact.workflow_stages_and_statuses:
                if sas[1] == 'QUEUED' and sas[2] == 'USEQ - Process Raw Data':
                    step_uri = sas[0].step.uri

            xml = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
                    <tmp:step-creation xmlns:tmp="http://genologics.com/ri/step">
                        <configuration uri="{step_uri}"/>
                        <container-type>Tube</container-type>
                        <inputs>
                            <input uri="{artifact.uri}" replicates="1"/>
                        </inputs>
                    </tmp:step-creation>
                    '''
            output = lims.post(
                uri=lims.get_uri('steps'),
                data=xml
            )
            step_details_uri = output.find('details').get('uri')
            step_details = StepDetails(lims, uri=step_details_uri)
            for io_map in step_details.input_output_maps:
                if io_map[1]['output-generation-type'] == 'PerInput':
                    artifact = io_map[1]['uri'] #output artifact
                    artifact.udf['Sequencing Succesful'] = True if succesful else False
                    artifact.put()
                    actions = [{'action':'nextstep','artifact':artifact, 'step-uri':analysis_step_uri}]

            step_action_uri = output.find('actions').get('uri')
            step_actions = StepActions(lims, uri=step_action_uri)
            step = step_actions.step
            step.advance()

            step_actions.set_next_actions(actions)
            step_actions.put()
            step.advance()


    session,Run, IlluminaSequencingStats,NanoporeSequencingStats = createDBSession()
    portal_run = session.query(Run).filter_by(run_id=project_id).first()
    if portal_run.platform == 'Oxford Nanopore':
        stats = session.query(NanoporeSequencingStats).filter_by(flowcell_id=fid).first()
        if not stats:
            sys.exit(f"Error : No existing stats found in portal for {project_id}")

        stats.succesful = True if succesful else False
        session.commit()
    else:
        stats = session.query(IlluminaSequencingStats).filter_by(flowcell_id=fid).first()
        if not stats:
            sys.exit(f"Error : No existing stats found in portal for {project_id}")

        stats.succesful = True if succesful else False
        session.commit()

def run(lims, project_id, fid,succesful):
    move_project(lims,project_id, fid,succesful)
