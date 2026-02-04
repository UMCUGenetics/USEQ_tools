"""Module for collecting and reporting sample measurements from LIMS."""

import sys
from typing import Dict, Any, TextIO, Optional

from genologics.entities import Project
from genologics.lims import Lims
from config import Config


def get_sample_measurements(lims: Lims, project_id: str, output_file: Optional[TextIO] = None) -> Dict[str, Dict[str, Any]]:
    """
    Collect measurement data for samples and pools from a LIMS project.

    This function retrieves all samples from a project and collects various
    quality control measurements from different process stages, including
    isolation, library preparation, and QC steps. Measurements are organized
    by sample name and pool.

    Args:
        lims (Lims): LIMS instance
        project_id (str): LIMS project ID.
        output_file (TextIO): Optional file object to write measurements to. If provided
            and the function is called from 'run', measurements will be written
            to this file.

    Returns:
        A dictionary containing collected measurements with the structure:
        {
            'samples': {
                'sample_name': {
                    'Isolated conc. (ng/ul)': value,
                    'Pre library prep conc. (ng/ul)': value,
                    'Post library prep conc. (ng/ul)': value,
                    'RIN': value
                }
            },
            'pool': {
                'Library conc. (ng/ul)': value,
                'Average length (bp)': value
            }
        }

    Note:
        Measurements are extracted from specific UDF (User-Defined Field) values
        associated with artifacts at different process stages. Missing values
        are marked as 'NA'.
    """
    collected_measurements = {'samples': {}, 'pool': {}}
    project = Project(lims, id=project_id)
    samples = lims.get_samples(projectlimsid=project_id)

    for sample in samples:
        if sample.name not in collected_measurements['samples']:
            collected_measurements['samples'][sample.name] = {}

        sample_artifacts = lims.get_artifacts(samplelimsid=sample.id)

        for sample_artifact in sample_artifacts:
            if not sample_artifact.parent_process:
                continue

            parent_process = sample_artifact.parent_process
            process_name = parent_process.type.name
            all_udfs = dict(sample_artifact.udf.items())

            if not all_udfs:
                continue

            if process_name in Config.ISOLATION_PROCESSES:
                collected_measurements['samples'][sample.name][
                    'Isolated conc. (ng/ul)'] = all_udfs.get(
                    'Concentration Qubit QC (DNA) 5.0 (ng/ul)', 'NA')

            if process_name in ['USEQ - Pre LibPrep QC']:
                collected_measurements['samples'][sample.name][
                    'Pre library prep conc. (ng/ul)'] = all_udfs.get(
                    'Concentration Qubit QC (DNA) 5.0 (ng/ul)', 'NA')
                collected_measurements['samples'][sample.name][
                    'RIN'] = all_udfs.get('RIN', 'NA')

            if process_name in ['USEQ - Post LibPrep QC']:
                collected_measurements['samples'][sample.name][
                    'Post library prep conc. (ng/ul)'] = all_udfs.get(
                    'Concentration Qubit QC (DNA) 5.0 (ng/ul)', 'NA')

            if process_name in ['USEQ - Qubit QC']:
                collected_measurements['pool'][
                    'Library conc. (ng/ul)'] = all_udfs.get(
                    'Concentration Qubit QC (DNA) 5.0 (ng/ul)', 'NA')

            if process_name in ['USEQ - Bioanalyzer QC DNA',
                                'USEQ - Bioanalyzer QC RNA']:
                collected_measurements['pool'][
                    'Average length (bp)'] = all_udfs.get(
                    'Average length (bp)', 'NA')

    # Write to output file if called from run function
    calling_function = sys._getframe(1).f_code.co_name
    if calling_function == 'run' and output_file:
        for sample in collected_measurements['samples']:
            output_file.write(f"{sample}\n")
            for measurement_name, measurement_value in (
                collected_measurements['samples'][sample].items()
            ):
                output_file.write(
                    f"\t{measurement_name} : {measurement_value}\n")

        output_file.write("pool\n")
        for measurement_name, measurement_value in (
            collected_measurements['pool'].items()
        ):
            output_file.write(
                f"\t{measurement_name} : {measurement_value}\n")

    return collected_measurements


def run(lims: Lims, project_id: str, output_file: TextIO) -> Dict[str, Dict[str, Any]]:
    """
    Execute the sample measurements collection process.

    Args:
        lims (Lims): LIMS instance
        project_id (str): LIMS project ID.
        output_file (TextIO): Optional file object to write measurements to. If provided
            and the function is called from 'run', measurements will be written
            to this file.

    Returns:
        A dictionary containing collected measurements for samples and pools.
    """
    return get_sample_measurements(lims, project_id, output_file)
