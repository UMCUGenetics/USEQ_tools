from genologics.entities import Artifact,StepDetails
from modules.useq_template import TEMPLATE_PATH,TEMPLATE_ENVIRONMENT,renderTemplate
from modules.useq_mail import sendMail

def run_started(lims, sender, receivers, step_uri):
    """Sends a run started mail"""

    #Get the flow cell id
    step_details = StepDetails(lims, uri=step_uri+'/details')
    flow_cell_id = step_details.udf['Flow Cell ID']

    #Get all input artifacts
    input_artifacts = []
    run_samples = []
    client = ''
    platform = ''
    for input_output in step_details.input_output_maps:
        artifact = input_output[0]['uri'] #input artifact
        if artifact not in input_artifacts:
            input_artifacts.append(artifact)
            for sample in artifact.samples:
                run_samples.append({
                    'name' : sample.name,
                    'project_name' : sample.project.name,
                    'project_id' : sample.project.id,
                    'analysis' : sample.udf['Analysis'],
                    'reference' : sample.udf['Reference Genome']
                })
                client = sample.project.researcher
                platform = sample.udf['Platform']
    content = renderTemplate('run_started_template.html', {'samples' : run_samples, 'client' : client})
    subject = "An {0} run for project {1} ({2}) was just started on {3}".format(platform, run_samples[0]['project_name'], run_samples[0]['project_id'], flow_cell_id)

    sendMail(subject, content, sender, receivers, None)

def run_finished(lims, sender, receivers, step_uri):
    """Sends a run finished mail formatted for Trello"""
    pass
    # step_details = StepDetails(lims, uri=step_uri+'/details')
    #
    # input_artifacts = []
    # run_samples = []
    # client = ''
    # platform = ''
    # #Get all input artifacts
    # for input_output in step_details.input_output_maps:
    #     artifact = input_output[0]['uri'] #input artifact
    #     if artifact not in input_artifacts:
    #         input_artifacts.append(artifact)
    #         for sample in artifact.samples:
    #             run_samples.append({
    #                 'name' : sample.name,
    #                 'project_name' : sample.project.name,
    #                 'project_id' : sample.project.id,
    #                 'analysis' : sample.udf['Analysis'],
    #                 'reference' : sample.udf['Reference Genome']
    #             })
    #             client = sample.project.researcher
    #             platform = sample.udf['Platform']


def run(lims, sender, receivers, mode, step_uri=None):
    """Sends a run status mail, type is determined by mode"""
    globals()[mode](lims,sender,receivers, step_uri=step_uri)
