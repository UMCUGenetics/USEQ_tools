from genologics.entities import Project,Step
from config import Config
import sys



def route(lims, project_id, protocol_type):
    project = None
    try:
        project = Project(lims, id=project_id)
        project_name = project.name
    except:
        sys.exit(f"Error : Project ID {project_id} not found in LIMS!")


    samples = lims.get_samples(projectlimsid=project.id)
    to_route = {}
    for sample in samples:
        stage = None

        if protocol_type == 'ISOLATION':
            stage = Config.WORKFLOW_STEPS['SEQUENCING']['steps']['ISOLATION']['stage_nrs'][ 'USEQ - Isolation' ]
        elif protocol_type == 'LIBPREP':
            if 'Library prep kit' in sample.udf:
                # next_step = STEP_URIS[ first_sample.udf['Library prep kit'] ]
                stage = Config.WORKFLOW_STEPS['SEQUENCING']['steps']['LIBPREP']['stage_nrs'][ sample.udf['Library prep kit'] ]
            else:
                if sample.udf['Platform'] == 'Oxford Nanopore':
                    if sample.udf['Sample Type'] == 'RNA total isolated':
                        # next_step = STEP_URIS['USEQ - LIBPREP-ONT-RNA']
                        stage = Config.WORKFLOW_STEPS['SEQUENCING']['steps']['LIBPREP']['stage_nrs'][ 'USEQ - LIBPREP-ONT-RNA' ]
                    else:
                        # next_step = STEP_URIS['USEQ - LIBPREP-ONT-DNA']
                        stage = Config.WORKFLOW_STEPS['SEQUENCING']['steps']['LIBPREP']['stage_nrs'][ 'USEQ - LIBPREP-ONT-DNA' ]
        elif protocol_type == 'POOLING':
            stage = Config.WORKFLOW_STEPS['SEQUENCING']['steps']['POOLING']['stage_nrs'][ 'USEQ - Library Pooling' ]
        elif protocol_type == 'POOL QC':
            stage = Config.WORKFLOW_STEPS['SEQUENCING']['steps']['POOL QC']['stage_nrs'][ 'USEQ - Pool QC' ]
        elif protocol_type == 'ILLUMINA SEQUENCING':
            platform = sample.udf['Platform']
            stage = Config.WORKFLOW_STEPS['SEQUENCING']['steps']['ILLUMINA SEQUENCING']['stage_nrs'][platform]
        elif protocol_type == 'NANOPORE SEQUENCING':
            stage = Config.WORKFLOW_STEPS['SEQUENCING']['steps']['NANOPORE SEQUENCING']['stage_nrs']['Oxford Nanopore']
        elif protocol_type == 'POST SEQUENCING':
            stage = Config.WORKFLOW_STEPS['SEQUENCING']['steps']['POST SEQUENCING']['stage_nrs']['USEQ - Post Sequencing']

        if stage not in to_route:
            to_route[stage] = []

        to_route[stage].append(sample.artifact)

    for step, artifact_list in to_route.items():
        workflow_nr, stage = step.split(":")
        uri = f"{Config.LIMS_URI}/api/v2/configuration/workflows/{workflow_nr}/stages/{stage}"
        lims.route_artifacts(artifact_list,stage_uri=uri)

def run(lims, project_id, protocol_type):
    route(lims,project_id, protocol_type)
