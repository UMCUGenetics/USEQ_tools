"""Module for closing LIMS projects."""

import sys
from datetime import datetime
from typing import Optional

from genologics.entities import Project, Step
from genologics.lims import Lims


def close_projects(lims: Lims, step_uri: str):
    """
    Close all projects included in a step.

    Sets the close date to today's date for all projects that don't already have a close date.

    Args:
        lims (Lims): LIMS instance.
        step_uri (str): URI of the step containing samples whose projects should be closed.
    """
    step = Step(lims, uri=step_uri)

    for io_map in step.details.input_output_maps:
        input_artifact = io_map[0]["uri"]
        first_sample = input_artifact.samples[0]
        project = first_sample.project

        if not project.close_date:
            try:
                project.close_date = datetime.today().strftime("%Y-%m-%d")
                project.put()
            except Exception as e:
                print(f"Failed to close project {project.id}: {e}")


def close_project(lims: Lims, project_id: str):
    """
    Close a specific project by ID.

    Sets the close date to today's date if the project doesn't already have a close date.

    Args:
        lims (Lims): LIMS instance.
        project_id (str): ID of LIMS project.
    """
    project = Project(lims, id=project_id)

    if not project.close_date:
        try:
            project.close_date = datetime.today().strftime("%Y-%m-%d")
            project.put()
        except Exception as e:
            print(f"Failed to close project {project_id}: {e}")


def run(lims: Lims, step_uri: Optional[str] = None, project_id: Optional[str] = None):
    """
    Execute project closing operation.

    If project_id is provided, closes that specific project. Otherwise, closes all
    projects associated with samples in the specified step.

    Args:
        lims (Lims): LIMS instance for API interaction.
        step_uri (str): URI of the step (required if project_id is not provided).
        project_id (str): ID of a specific project to close (optional).

    Raises:
        ValueError: If neither project_id nor step_uri is provided.
    """
    if project_id:
        close_project(lims, project_id)
    elif step_uri:
        close_projects(lims, step_uri)
    else:
        raise ValueError("Either project_id or step_uri must be provided")
