from genologics.entities import Step,Project
import datetime

def closeProjects(lims, step_uri):
    step = Step(lims, uri=step_uri)

    for io_map in step.details.input_output_maps:
        input_artifact = io_map[0]['uri']

        first_sample = input_artifact.samples[0]
        project = first_sample.project

        if not project.close_date:
            try:
                project.close_date = datetime.datetime.today().strftime('%Y-%m-%d')
                project.put()
            except:
                print ('Failed to close project {0}'.format(project_id))

def closeProject(lims, id):
    project = Project(lims, id=id)

    if not project.close_date:
        try:
            project.close_date = datetime.datetime.today().strftime('%Y-%m-%d')
            project.put()
        except:
            print ('Failed to close project {0}'.format(project_id))

def run(lims, step_uri, project_id):
    """Runs the closeProjects function"""
    if project_id :
        closeProject(lims, project_id)
    else:
        closeProjects(lims, step_uri)
