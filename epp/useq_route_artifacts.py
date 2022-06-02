from genologics.entities import Step
from config import Config

from epp.useq_run_status_mail import run_finished

def routeArtifacts(lims, step_uri, input):
    step = Step(lims, uri=step_uri)

    current_step = step.configuration.name
    print(current_step)
    to_route = {}
    for io_map in step.details.input_output_maps:
        artifact = None

        next_stage = None
        if input:
            artifact = io_map[0]['uri'] #input artifact
        else:
            artifact = io_map[1]['uri'] #output artifact

        first_sample = artifact.samples[0]

        if current_step in Config.WORKFLOW_STEPS['SEQUENCING']['steps']['ISOLATION']['names']:

            if 'Library prep kit' in first_sample.udf:
                # next_step = STEP_URIS[ first_sample.udf['Library prep kit'] ]
                next_stage = Config.WORKFLOW_STEPS['SEQUENCING']['steps']['ISOLATION']['stage_nrs'][ first_sample.udf['Library prep kit'] ]
            else:
                if first_sample.udf['Platform'] == 'Oxford Nanopore':
                    if first_sample.udf['Sample Type'] == 'RNA total isolated':
                        # next_step = STEP_URIS['USEQ - LIBPREP-ONT-RNA']
                        next_stage = Config.WORKFLOW_STEPS['SEQUENCING']['steps']['ISOLATION']['stage_nrs'][ 'USEQ - LIBPREP-ONT-RNA' ]
                    else:
                        # next_step = STEP_URIS['USEQ - LIBPREP-ONT-DNA']
                        next_stage = Config.WORKFLOW_STEPS['SEQUENCING']['steps']['ISOLATION']['stage_nrs'][ 'USEQ - LIBPREP-ONT-DNA' ]
                else:
                    # next_step = STEP_URIS[ 'USEQ - Fingerprinting' ]
                    next_stage = Config.WORKFLOW_STEPS['FINGERPRINTING']['steps']['FINGERPRINTING']['stage_nrs'][ 'USEQ - Fingerprinting' ]
        elif current_step in Config.WORKFLOW_STEPS['SEQUENCING']['steps']['LIBPREP']['names']:
            # next_step = STEP_URIS[ 'USEQ - Library Pooling' ]
            next_stage = Config.WORKFLOW_STEPS['SEQUENCING']['steps']['POOLING']['stage_nrs'][ 'USEQ - Library Pooling' ]
            print(next_stage)
        elif current_step in Config.WORKFLOW_STEPS['SEQUENCING']['steps']['POOLING']['names']:
            sample_type = first_sample.udf['Sample Type']
            if sample_type == 'DNA library' or sample_type == 'RNA library': #Go to pool QC
                # next_step = STEP_URIS['USEQ - Pool QC']
                next_stage = Config.WORKFLOW_STEPS['SEQUENCING']['steps']['POOL QC']['stage_nrs'][ 'USEQ - Pool QC' ]
            else:#Pool QC has already been done
                platform = first_sample.udf['Platform']
                if platform == 'Oxford Nanopore':
                    next_stage = Config.WORKFLOW_STEPS['SEQUENCING']['steps']['NANOPORE SEQUENCING']['stage_nrs']['Oxford Nanopore']
                else:
                    # next_step = STEP_URIS[ first_sample.udf['Platform'] ]
                    next_stage = next_stage = Config.WORKFLOW_STEPS['SEQUENCING']['steps']['ILLUMINA SEQUENCING']['stage_nrs'][platform]


        elif current_step in Config.WORKFLOW_STEPS['SEQUENCING']['steps']['POOL QC']['names']:
            # next_step = STEP_URIS[ first_sample.udf['Platform'] ]
            platform = first_sample.udf['Platform']
            if platform == 'Oxford Nanopore':
                next_stage = Config.WORKFLOW_STEPS['SEQUENCING']['steps']['NANOPORE SEQUENCING']['stage_nrs']['Oxford Nanopore']
            else:
                # next_step = STEP_URIS[ first_sample.udf['Platform'] ]
                next_stage = Config.WORKFLOW_STEPS['SEQUENCING']['steps']['ILLUMINA SEQUENCING']['stage_nrs'][platform]
        elif current_step in Config.WORKFLOW_STEPS['SEQUENCING']['steps']['ILLUMINA SEQUENCING']['names'] or current_step in Config.WORKFLOW_STEPS['SEQUENCING']['steps']['NANOPORE SEQUENCING']['names']:
            # next_step = STEP_URIS['USEQ - Post Sequencing']
            next_stage = Config.WORKFLOW_STEPS['SEQUENCING']['steps']['POST SEQUENCING']['stage_nrs']['USEQ - Post Sequencing']

        elif current_step in Config.WORKFLOW_STEPS['SEQUENCING']['steps']['POST SEQUENCING']['names']:
            sample_analyses = None
            if 'Analysis' in first_sample.udf:
                sample_analyses = first_sample.udf['Analysis'].split(",")

            if not sample_analyses:
                # next_step = STEP_URIS['USEQ - Encrypt & Send']
                next_stage = Config.WORKFLOW_STEPS['SEQUENCING']['steps']['POST SEQUENCING']['stage_nrs']['USEQ - Encrypt & Send']
            elif len(sample_analyses) == 1 and 'Raw data (FastQ)' in sample_analyses:
                # next_step = STEP_URIS['USEQ - Encrypt & Send']
                next_stage = Config.WORKFLOW_STEPS['SEQUENCING']['steps']['POST SEQUENCING']['stage_nrs']['USEQ - Encrypt & Send']
            else:
                # next_step = STEP_URIS['USEQ - Analysis']
                next_stage = Config.WORKFLOW_STEPS['SEQUENCING']['steps']['POST SEQUENCING']['stage_nrs']['USEQ - Analysis']
                run_finished(lims,Config.MAIL_SENDER, Config.TRELLO_ANALYSIS_BOARD, artifact )


        if next_stage not in to_route:
            to_route[ next_stage ] = []
        to_route[ next_stage ].append( artifact)

    for step, artifact_list in to_route.items():
        workflow_nr, stage = step.split(":")
        uri = f"{Config.LIMS_URI}/api/v2/configuration/workflows/{workflow_nr}/stages/{stage}"
        # https://usf-lims.umcutrecht.nl/api/v2/configuration/workflows/851/stages/3915
        lims.route_artifacts(artifact_list,stage_uri=uri)

def run(lims, step_uri, input):
    """Runs the routeArtifacts function"""
    routeArtifacts(lims, step_uri, input)
