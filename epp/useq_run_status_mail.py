"""Module for sending run status notification emails."""

from typing import Any, Dict, List, Optional

from genologics.entities import Artifact, StepDetails
from genologics.lims import Lims

from modules.useq_mail import send_mail
from modules.useq_template import TEMPLATE_ENVIRONMENT, TEMPLATE_PATH, render_template

def run_started(lims: Lims, sender: str, receivers: List[str], step_uri: str):
    """
    Send a run started notification email.

    Args:
        lims: LIMS instance.
        sender: Email address of the sender.
        receivers: List of recipient email addresses.
        step_uri: URI of the step that was started.
    """
    step_details = StepDetails(lims, uri=f"{step_uri}/details")
    flow_cell_id = step_details.udf["Flow Cell ID"]

    input_artifacts = []
    run_samples = []
    client = ""
    platform = ""

    for input_output in step_details.input_output_maps:
        artifact = input_output[0]["uri"]  # input artifact

        if artifact not in input_artifacts:
            input_artifacts.append(artifact)

            for sample in artifact.samples:
                run_samples.append({
                    "name": sample.name,
                    "project_name": sample.project.name,
                    "project_id": sample.project.id,
                    "analysis": sample.udf["Analysis"],
                    "reference": sample.udf["Reference Genome"],
                })
                client = sample.project.researcher
                platform = sample.udf["Platform"]

    content = render_template(
        "run_started_template.html", {"samples": run_samples, "client": client}
    )
    subject = (
        f"An {platform} run for project {run_samples[0]['project_name']} "
        f"({run_samples[0]['project_id']}) was just started on {flow_cell_id}"
    )
    send_mail(subject, content, sender, receivers, None)


def run_finished(lims: Lims, sender: str, receivers: List[str], artifact: Artifact):
    """
    Send a run finished notification email formatted for Trello.

    Args:
        lims: LIMS instance.
        sender: Email address of the sender.
        receivers: List of recipient email addresses.
        artifact: Artifact associated with the finished run.
    """
    run_samples = []
    client = ""
    platform = ""

    for sample in artifact.samples:
        run_samples.append({
            "name": sample.name,
            "project_name": sample.project.name,
            "project_id": sample.project.id,
            "analysis": sample.udf.get("Analysis", "SNP Fingerprinting"),
            "reference": sample.udf.get("Reference Genome", "Human - GRCh38"),
        })
        client = sample.project.researcher
        platform = sample.udf["Platform"]

    references = ", ".join(set(sample["reference"] for sample in run_samples))

    content = render_template(
        "run_finished_template.html",
        {
            "nr_samples": len(run_samples),
            "project_name": run_samples[0]["project_name"],
            "project_id": run_samples[0]["project_id"],
            "analysis": run_samples[0]["analysis"],
            "reference(s)": references,
            "client": client,
        },
    )
    subject = f"{run_samples[0]['project_id']} queued for analysis #Please_analyse"
    send_mail(subject, content, sender, receivers, attachments=None, logo=False)


def run(lims: Lims, sender: str, receivers: List[str], mode: str, step_uri: Optional[str] = None, artifact: Optional[Artifact] = None):
    """
    Send a run status notification email.

    The type of notification is determined by the mode parameter.

    Args:
        lims: LIMS instance.
        sender: Email address of the sender.
        receivers: List of recipient email addresses.
        mode: Type of notification ('run_started' or 'run_finished').
        step_uri: URI of the step (required for 'run_started' mode).
        artifact: Artifact object (required for 'run_finished' mode).

    Raises:
        KeyError: If mode does not match a valid function name.
    """
    if mode == "run_started":
        run_started(lims, sender, receivers, step_uri)
    elif mode == "run_finished":
        run_finished(lims, sender, receivers, artifact)
    else:
        raise ValueError(f"Invalid mode: {mode}")
