from genologics.entities import Step
from config import STEP_URIS,STEP_NAMES,MAIL_SENDER,MAIL_ANALYSIS

from epp.useq_run_status_mail import run_finished

def routeArtifacts(lims, step_uri, input):
    step = Step(lims, uri=step_uri)

    current_step = step.configuration.name
    print current_step
    to_route = {}
    for io_map in step.details.input_output_maps:
        artifact = None

        next_step = None
        if input:
            artifact = io_map[0]['uri'] #input artifact
        else:
            artifact = io_map[1]['uri'] #output artifact

        first_sample = artifact.samples[0]

        if current_step in STEP_NAMES['ISOLATION']:

            if 'Library prep kit' in first_sample.udf:
                next_step = STEP_URIS[ first_sample.udf['Library prep kit'] ]
            else:
                if first_sample.udf['Platform'] == 'Oxford Nanopore':
                    if first_sample.udf['Sample Type'] == 'RNA total isolated':
                        next_step = STEP_URIS['USEQ - LIBPREP-ONT-RNA']
                    else:
                        next_step = STEP_URIS['USEQ - LIBPREP-ONT-DNA']
                else:
                    next_step = STEP_URIS[ 'USEQ - Fingerprinting' ]
        elif current_step in STEP_NAMES['LIBPREP']:
            next_step = STEP_URIS[ 'USEQ - Library Pooling' ]
            # print 'Libprep, next step:',next_step
        elif current_step in STEP_NAMES['POOLING']:
            sample_type = first_sample.udf['Sample Type']
            if sample_type == 'DNA library' or sample_type == 'RNA library': #Go to pool QC
                next_step = STEP_URIS['USEQ - Pool QC']
            else:#Pool QC has already been done
                next_step = STEP_URIS[ first_sample.udf['Platform'] ]
            # print 'Pooling, next step:',next_step
        elif current_step in STEP_NAMES['POOL QC']:
            next_step = STEP_URIS[ first_sample.udf['Platform'] ]
            # print 'Pool QC, next step:',next_step
        elif current_step in STEP_NAMES['SEQUENCING']:
            next_step = STEP_URIS['USEQ - Post Sequencing']
            # print 'Sequencing, next step:',next_step
        elif current_step in STEP_NAMES['POST SEQUENCING']:
            sample_analyses = first_sample.udf['Analysis'].split(",")
            if len(sample_analyses) == 1 and 'Raw data (FastQ)' in sample_analyses:
                next_step = STEP_URIS['USEQ - Encrypt & Send']
            else:
                next_step = STEP_URIS['USEQ - Analysis']
                run_finished(lims,MAIL_SENDER, MAIL_ANALYSIS, artifact )
            # print 'Post sequencing, next step:',next_step

        if next_step not in to_route:
            to_route[ next_step ] = []
        to_route[ next_step ].append( artifact)

    for step, artifact_list in to_route.items():
        lims.route_artifacts(artifact_list,stage_uri=step)

def run(lims, step_uri, input):
    """Runs the routeArtifacts function"""
    routeArtifacts(lims, step_uri, input)
