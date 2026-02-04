"""Module for routing artifacts through LIMS workflow stages."""

import sys
from typing import Any, Dict, List

from genologics.entities import Artifact, Step, Sample
from genologics.lims import Lims

from config import Config
from epp.useq_run_status_mail import run_finished


def route_artifacts(lims: Lims, step_uri: str, use_input: bool):
    """
    Route artifacts to appropriate next workflow stages based on current step and sample properties.

    Args:
        lims (Lims): LIMS instance.
        step_uri (str): URI of the current step.
        use_input (bool): If True, route input artifacts; if False, route output artifacts.
    """
    step = Step(lims, uri=step_uri)
    current_step = step.configuration.name
    to_route = {}

    for io_map in step.details.input_output_maps:
        next_stages = []

        if use_input:
            artifact = io_map[0]["uri"]  # input artifact
        else:
            artifact = io_map[1]["uri"]  # output artifact

        first_sample = artifact.samples[0]

        # Route from ISOLATION step
        if current_step in Config.WORKFLOW_STEPS["SEQUENCING"]["steps"]["ISOLATION"]["names"]:
            next_stages.extend(_route_from_isolation(first_sample))

        # Route from LIBPREP step
        elif current_step in Config.WORKFLOW_STEPS["SEQUENCING"]["steps"]["LIBPREP"]["names"]:
            next_stages.append(
                Config.WORKFLOW_STEPS["SEQUENCING"]["steps"]["POOLING"]["stage_nrs"]["USEQ - Library Pooling"]
            )

        # Route from POOLING step
        elif current_step in Config.WORKFLOW_STEPS["SEQUENCING"]["steps"]["POOLING"]["names"]:
            next_stages.extend(_route_from_pooling(step, first_sample, io_map))

        # Route from POOL QC step
        elif current_step in Config.WORKFLOW_STEPS["SEQUENCING"]["steps"]["POOL QC"]["names"]:
            next_stages.extend(_route_from_pool_qc(first_sample, io_map))

        # Route from SEQUENCING steps
        elif (current_step in Config.WORKFLOW_STEPS["SEQUENCING"]["steps"]["ILLUMINA SEQUENCING"]["names"] or
              current_step in Config.WORKFLOW_STEPS["SEQUENCING"]["steps"]["NANOPORE SEQUENCING"]["names"]):
            next_stages.append(
                Config.WORKFLOW_STEPS["SEQUENCING"]["steps"]["POST SEQUENCING"]["stage_nrs"]["USEQ - Post Sequencing"]
            )

        # Route from POST SEQUENCING step
        elif current_step in Config.WORKFLOW_STEPS["SEQUENCING"]["steps"]["POST SEQUENCING"]["names"]:
            next_stages.extend(_route_from_post_sequencing(lims, current_step, first_sample, artifact))

        # Add artifact to routing dictionary for each next stage
        for next_stage in next_stages:
            if next_stage not in to_route:
                to_route[next_stage] = []
            to_route[next_stage].append(artifact)

    # Perform the actual routing
    for step_stage, artifact_list in to_route.items():
        workflow_nr, stage = step_stage.split(":")
        uri = f"{Config.LIMS_URI}/api/v2/configuration/workflows/{workflow_nr}/stages/{stage}"
        lims.route_artifacts(artifact_list, stage_uri=uri)


def _route_from_isolation(first_sample: Sample) -> List[str]:
    """
    Internal function to determine next stages when routing from ISOLATION step.

    Args:
        first_sample (Sample): LIMS sample object.

    Returns:
        List of next stage identifiers.
    """
    next_stages = []

    if "Library prep kit" in first_sample.udf:
        next_stages.append(
            Config.WORKFLOW_STEPS["SEQUENCING"]["steps"]["LIBPREP"]["stage_nrs"][first_sample.udf["Library prep kit"]]
        )
    else:
        platform = first_sample.udf["Platform"]

        if platform in ["Oxford Nanopore", "MinIon", "PromethIon"]:
            if first_sample.udf["Sample Type"] == "RNA total isolated":
                next_stages.append(
                    Config.WORKFLOW_STEPS["SEQUENCING"]["steps"]["LIBPREP"]["stage_nrs"]["USEQ - LIBPREP-ONT-RNA"]
                )
            else:
                next_stages.append(
                    Config.WORKFLOW_STEPS["SEQUENCING"]["steps"]["LIBPREP"]["stage_nrs"]["USEQ - LIBPREP-ONT-DNA"]
                )
        elif platform == "SNP Fingerprinting":  # old fingerprinting workflow
            next_stages.append(
                Config.WORKFLOW_STEPS["FINGERPRINTING"]["steps"]["FINGERPRINTING"]["stage_nrs"]["USEQ - Fingerprinting"]
            )
        elif platform == "60 SNP NimaGen panel":
            next_stages.append(
                Config.WORKFLOW_STEPS["SEQUENCING"]["steps"]["LIBPREP"]["stage_nrs"]["USEQ - LIBPREP-FINGERPRINTING"]
            )

    return next_stages


def _route_from_pooling(step: Step, first_sample: Sample, io_map: Dict[str, Any]) -> List[str]:
    """
    Internal function to determine next stages when routing from POOLING step.

    Args:
        step (Step): LIMS step object.
        first_sample (Sample): LIMS sample object.
        io_map (Dict[str, Any]): Input/output mapping dictionary.

    Returns:
        List of next stage identifiers.
    """
    next_stages = []
    sample_type = first_sample.udf["Sample Type"]

    if sample_type in ["DNA library", "RNA library"]:  # Go to pool QC
        next_stages.append(
            Config.WORKFLOW_STEPS["SEQUENCING"]["steps"]["POOL QC"]["stage_nrs"]["USEQ - Pool QC"]
        )
    else:  # Pool QC has already been done
        platform = _get_platform(step, first_sample)

        if platform in ["Oxford Nanopore", "MinIon", "PromethIon"]:
            next_stages.append(
                Config.WORKFLOW_STEPS["SEQUENCING"]["steps"]["NANOPORE SEQUENCING"]["stage_nrs"]["Oxford Nanopore"]
            )
        elif platform == "10X Chromium iX Single Cell":
            sequencing_platform = io_map[0]["uri"].parent_process.udf.get("Sequencing Platform", None)
            next_stages.append(
                Config.WORKFLOW_STEPS["SEQUENCING"]["steps"]["ILLUMINA SEQUENCING"]["stage_nrs"][sequencing_platform]
            )
        else:
            next_stages.append(
                Config.WORKFLOW_STEPS["SEQUENCING"]["steps"]["ILLUMINA SEQUENCING"]["stage_nrs"][platform]
            )

    return next_stages


def _route_from_pool_qc(first_sample: Sample, io_map: Dict[str, Any]) -> List[str]:
    """
    Internal function to determine next stages when routing from POOL QC step.

    Args:
        first_sample (Sample): LIMS Sample object
        io_map (Dict[str, Any]): Input/output mapping dictionary.

    Returns:
        List of next stage identifiers.
    """
    next_stages = []

    if "Sequencing Platform" in io_map[0]["uri"].parent_process.udf:
        platform = io_map[0]["uri"].parent_process.udf["Sequencing Platform"]
    else:
        platform = first_sample.udf["Platform"]

    if platform in ["Oxford Nanopore", "MinIon", "PromethIon"]:
        next_stages.append(
            Config.WORKFLOW_STEPS["SEQUENCING"]["steps"]["NANOPORE SEQUENCING"]["stage_nrs"]["Oxford Nanopore"]
        )
    else:
        next_stages.append(
            Config.WORKFLOW_STEPS["SEQUENCING"]["steps"]["ILLUMINA SEQUENCING"]["stage_nrs"][platform]
        )

    return next_stages


def _route_from_post_sequencing(lims: Lims, current_step: str, first_sample: Sample, artifact: Artifact) -> List[str]:
    """
    Internal function to determine next stages when routing from POST SEQUENCING step.

    Args:
        lims (Lims): LIMS instance.
        current_step (str): Name of the current step.
        first_sample (Sample): LIMS Sample object.
        artifact (Artifact): LIMS Artifact.

    Returns:
        List of next stage identifiers.
    """
    next_stages = []
    platform = first_sample.udf["Platform"]
    sample_analyses = []

    if "Analysis" in first_sample.udf:
        sample_analyses = first_sample.udf["Analysis"].split(",")
        if "Raw data (FastQ)" in sample_analyses:
            sample_analyses.remove("Raw data (FastQ)")

    if current_step in ["USEQ - BCL to FastQ", "USEQ - Process Raw Data"]:
        # Send project to both analysis and billing step
        if platform in ["60 SNP NimaGen panel", "Chromium X"] or sample_analyses:
            next_stages.append(
                Config.WORKFLOW_STEPS["SEQUENCING"]["steps"]["POST SEQUENCING"]["stage_nrs"]["USEQ - Analysis"]
            )
            run_finished(lims, Config.MAIL_SENDER, Config.TRELLO_ANALYSIS_BOARD, artifact)

        next_stages.append(
            Config.WORKFLOW_STEPS["SEQUENCING"]["steps"]["POST SEQUENCING"]["stage_nrs"]["USEQ - Ready for billing"]
        )

    return next_stages


def _get_platform(step: Step, first_sample: Sample) -> str:
    """
    Internal function to get the sequencing platform from step UDF or sample UDF.

    Args:
        step (Step): LIMS step object.
        first_sample (Sample): LIMS sample object.

    Returns:
        Platform name as string.
    """
    if "Sequencing Platform" in step.details.udf and step.details.udf["Sequencing Platform"]:
        return step.details.udf["Sequencing Platform"]
    return first_sample.udf["Platform"]


def run(lims: Lims, step_uri: str, use_input: bool):
    """
    Execute artifact routing operation.

    Args:
        lims (Lims): LIMS instance.
        step_uri (str): URI of the current step.
        use_input (bool): If True, route input artifacts; if False, route output artifacts.
    """
    route_artifacts(lims, step_uri, use_input)
