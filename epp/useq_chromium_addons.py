"""Module for renaming samples based on processing add-ons in LIMS."""

import sys

from genologics.entities import Artifact, Step
from genologics.lims import Lims

from typing import Dict, List, Tuple

# UDF name constants
UDF_BCR = 'BCR'
UDF_TCR = 'TCR'
UDF_CELL_SURFACE_PROTEIN = 'Cell surface protein'
UDF_CRISPR = 'CRISPR'

# Add-on suffix constants
SUFFIX_BCR = 'BCR'
SUFFIX_TCR = 'TCR'
SUFFIX_CSP = 'CSP'
SUFFIX_CRISPR = 'CRISPR'


def _get_add_ons(step: Step) -> List[str]:
    """Determine which add-on suffixes to apply based on step UDFs.

    Args:
        step (Step): LIMS step object

    Returns:
        List of add-on suffixes, starting with an empty string for base sample
    """
    add_ons = ['']

    if step.details.udf.get(UDF_BCR):
        add_ons.append(SUFFIX_BCR)
    if step.details.udf.get(UDF_TCR):
        add_ons.append(SUFFIX_TCR)
    if step.details.udf.get(UDF_CELL_SURFACE_PROTEIN):
        add_ons.append(SUFFIX_CSP)
    if step.details.udf.get(UDF_CRISPR):
        add_ons.append(SUFFIX_CRISPR)

    return add_ons


def _map_input_output_artifacts(step: Step) -> Tuple[Dict[Artifact, List[Artifact]], int, int]:
    """Map input artifacts to their output artifacts.

    Args:
        step (Step): LIMS step object

    Returns:
        Tuple of (artifacts_to_update dict, nr_input_samples, nr_derived_samples)
    """
    artifacts_to_update = {}
    nr_derived_samples = 0
    nr_input_samples = 0

    for io_map in step.details.input_output_maps:
        input_artifact = io_map[0]['uri']
        output_artifact = io_map[1]

        if output_artifact['output-generation-type'] == 'PerInput':
            if input_artifact not in artifacts_to_update:
                artifacts_to_update[input_artifact] = []
                nr_input_samples += 1

            nr_derived_samples += 1
            artifacts_to_update[input_artifact].append(output_artifact['uri'])

    return artifacts_to_update, nr_input_samples, nr_derived_samples


def _validate_add_ons_count(add_ons: List[str], nr_derived_samples: int, nr_input_samples: int):
    """Validate that the number of add-ons matches derived samples per input.

    Args:
        add_ons (List[str]): List of add-on suffixes
        nr_derived_samples (int): Total number of derived samples
        nr_input_samples (int): Total number of input samples

    Raises:
        SystemExit: If counts don't match
    """
    expected_derived_per_input = nr_derived_samples / nr_input_samples

    if len(add_ons) != expected_derived_per_input:
        sys.exit(
            f'The number of derived samples (per input sample) is not equal '
            f'to the number of add-ons ({len(add_ons)})'
        )


def _rename_artifacts(artifacts_to_update: Dict[Artifact, List[Artifact]], add_ons: List[str]) -> List[Artifact]:
    """Rename output artifacts based on input names and add-on suffixes.

    Args:
        artifacts_to_update (Dict[Artifact, List[Artifact]]): Dictionary mapping input artifacts to output artifacts
        add_ons (List[str]): List of add-on suffixes

    Returns:
        List of artifacts to be batch updated
    """
    artifact_batch = []

    for input_artifact in artifacts_to_update:
        for idx, output_artifact in enumerate(artifacts_to_update[input_artifact]):
            if not add_ons[idx]:
                output_artifact.name = input_artifact.name
            else:
                output_artifact.name = f"{input_artifact.name}-{add_ons[idx]}"

            artifact_batch.append(output_artifact)

    return artifact_batch


def rename_samples(lims: Lims, step_uri: str):
    """Rename samples based on processing add-ons specified in step UDFs.

    This function generates new names for derived samples by appending add-on
    suffixes (BCR, TCR, CSP, CRISPR) based on which processing steps are enabled
    in the step's UDFs. The first derived sample keeps the original name.

    Args:
        lims (Lims): LIMS instance
        step_uri (str): URI of the processing step

    Raises:
        SystemExit: If the number of derived samples per input doesn't match
                   the number of add-ons
    """
    step = Step(lims, uri=step_uri)

    # Determine which add-ons are enabled
    add_ons = _get_add_ons(step)

    # Map input artifacts to their outputs
    artifacts_to_update, nr_input_samples, nr_derived_samples = (
        _map_input_output_artifacts(step)
    )

    # Validate configuration
    _validate_add_ons_count(add_ons, nr_derived_samples, nr_input_samples)

    # Rename and collect artifacts for batch update
    artifact_batch = _rename_artifacts(artifacts_to_update, add_ons)

    # Commit changes to LIMS
    lims.put_batch(artifact_batch)


def run(lims: Lims, step_uri: str):
    """Entry point for the sample renaming script.

    Args:
        lims: LIMS instance
        step_uri: URI of the processing step

    Raises:
        SystemExit: If the number of derived samples per input doesn't match the number of add-ons
    """
    rename_samples(lims, step_uri)
