"""Module for creating Illumina sequencing sample sheets."""

import sys
from typing import Any, Dict, List, Optional, TextIO

from genologics.entities import Process, Step, StepDetails, Sample, Artifact
from genologics.lims import Lims

from config import Config
from modules.useq_template import TEMPLATE_ENVIRONMENT, TEMPLATE_PATH, render_template

def create_samplesheet(lims: Lims, step_uri: str) -> str:
    """
    Create an Illumina sample sheet for a sequencing run.

    Generates a sample sheet based on the sequencing step configuration, including
    investigator information, read settings, and sample/index information for all
    samples in the run.

    Args:
        lims (Lims): LIMS instance for API interaction.
        step_uri (str): URI of the sequencing step.

    Returns:
        Rendered sample sheet as a string in CSV format.
    """
    uri_parts = step_uri.split("/")
    process_id = uri_parts[-1]
    process = Process(lims, id=process_id)

    # Initialize sample sheet data structure
    samplesheet_data: Dict[str, Any] = {
        "investigator_name": None,
        "experiment_name": None,
        "date": None,
        "lanes": False,
        "read1_cycles": 0,
        "read2_cycles": 0,
        "index1_cycles": 0,
        "index2_cycles": 0,
        "dual_index": False,
        "trim_umi": None,
        "samples": [],
    }

    # Get filtered input-output maps (analytes only)
    seq_io_maps = process.input_output_maps
    seq_io_maps_filtered = [
        iom for iom in seq_io_maps if iom[1]["output-type"] == "Analyte"
    ]

    if Config.DEVMODE:
        print(f"Processing {len(seq_io_maps_filtered)} input-output maps")

    # Set header information
    technician = process.technician
    samplesheet_data["investigator_name"] = f"{technician.first_name} {technician.last_name}"
    samplesheet_data["experiment_name"] = process.udf["Experiment Name"]
    samplesheet_data["date"] = process.date_run
    samplesheet_data["read1_cycles"] = process.udf["Read 1 Cycles"]

    if "Read 2 Cycles" in process.udf:
        samplesheet_data["read2_cycles"] = process.udf["Read 2 Cycles"]

    samplesheet_data["index1_cycles"] = int(process.udf.get("Index Read 1", 0))

    if "Index Read 2" in process.udf:
        samplesheet_data["index2_cycles"] = int(process.udf.get("Index Read 2", 0))

    if Config.DEVMODE:
        _log_samplesheet_header(samplesheet_data)

    # Process each pool in the sequencing run
    for seq_io_map in seq_io_maps_filtered:
        input_pool, lane_pool = [io["uri"] for io in seq_io_map]
        lane_placements = {
            v.id: k for k, v in lane_pool.container.placements.items()
        }

        # if len(lane_placements) > 1:
        #     samplesheet_data["lanes"] = True

        pooling_process = input_pool.parent_process

        # Process each sample in the pool
        for pooling_io_map in pooling_process.input_output_maps:
            input_sample_artifact, output_pool = [io["uri"] for io in pooling_io_map]
            input_sample = input_sample_artifact.samples[0]

            # Get index information
            index_name = input_sample_artifact.reagent_labels[0]
            reagent = lims.get_reagent_types(name=index_name)[0]
            index_seqs = reagent.sequence.split("-")
            index_cat = reagent.category
            project_id = input_sample.project.id

            # Calculate cycle settings for this sample
            sample_data = _calculate_sample_settings(
                input_sample,
                input_sample_artifact,
                lane_pool,
                index_seqs,
                index_cat,
                samplesheet_data,
                lane_placements,
                project_id,
            )

            samplesheet_data["samples"].append(sample_data)
    print(samplesheet_data)
    # Render and return the sample sheet
    samplesheet = render_template("SampleSheetv2_template.csv", samplesheet_data)
    return samplesheet


def _log_samplesheet_header(samplesheet_data: Dict[str, Any]):
    """
    Log sample sheet header information for debugging.

    Args:
        samplesheet_data (Dict[str, Any]): Dictionary containing sample sheet configuration.
    """
    print("Samplesheet header has following settings:")
    print(f"Investigator {samplesheet_data['investigator_name']}")
    print(f"Experiment Name {samplesheet_data['experiment_name']}")
    print(f"Date {samplesheet_data['date']}")
    print(
        f"Default Read Settings : {samplesheet_data['read1_cycles']};"
        f"{samplesheet_data['index1_cycles']};"
        f"{samplesheet_data['index2_cycles']};"
        f"{samplesheet_data['read2_cycles']};"
    )


def _calculate_sample_settings(input_sample: Sample, input_sample_artifact: Artifact, lane_pool: Artifact, index_seqs: List[str], index_cat: str,
    samplesheet_data: Dict[str, Any], lane_placements: Dict[str, str], project_id: str) -> Dict[str, Any]:
    """
    Calculate read and index settings for a single sample.

    Args:
        input_sample (Sample): Sample entity from LIMS.
        input_sample_artifact (Artifact): Artifact for the input sample.
        lane_pool (Artifact): Lane pool artifact.
        index_seqs (List[str]): List of index sequences.
        index_cat (str): Index reagent category.
        samplesheet_data (Dict[str, Any]): Overall sample sheet configuration.
        lane_placements (Dict[str, str]): Dictionary mapping artifact IDs to lane positions.
        project_id: LIMS Project ID for the sample.

    Returns:
        Dictionary containing sample configuration for the sample sheet.
    """
    # Set cycle defaults from sample sheet header
    read1_cycles = samplesheet_data["read1_cycles"]
    read2_cycles = samplesheet_data["read2_cycles"] if samplesheet_data["read2_cycles"] else 0
    index1_cycles = samplesheet_data["index1_cycles"]
    index2_cycles = samplesheet_data["index2_cycles"] if samplesheet_data["index2_cycles"] else 0

    # Override with customer-specified cycles if significantly different
    if "USEQ Read1 Cycles" in input_sample.udf:
        r1_customer = int(input_sample.udf["USEQ Read1 Cycles"])
        if abs(r1_customer - read1_cycles) > 1:
            read1_cycles = r1_customer

    if "USEQ Read2 Cycles" in input_sample.udf:
        r2_customer = int(input_sample.udf["USEQ Read2 Cycles"])
        if abs(r2_customer - read2_cycles) > 1:
            read2_cycles = r2_customer

    # Determine index cycles based on index sequences
    if len(index_seqs) == 2:  # Dual index
        samplesheet_data["dual_index"] = True
        index1_cycles = len(index_seqs[0])
        index2_cycles = len(index_seqs[1])
    else:
        index1_cycles = len(index_seqs[0])
        index2_cycles = 0

    # Override with lane-specific read settings if specified
    if "Read Settings" in lane_pool.udf and lane_pool.udf["Read Settings"]:
        read1_cycles, index1_cycles, index2_cycles, read2_cycles = [
            int(x) for x in lane_pool.udf["Read Settings"].split(";")
        ]

    # Build override cycles string
    override_cycles = _build_override_cycles(
        read1_cycles,
        read2_cycles,
        index1_cycles,
        index2_cycles,
        index_cat,
        samplesheet_data,
    )

    sample_id = input_sample_artifact.name
    lane = lane_placements[lane_pool.id].split(":")[0]

    if Config.DEVMODE:
        print(
            f"Processing sample {sample_id} with index {index_seqs} "
            f"for projectID {project_id} on lane {lane} "
            f"with settings {override_cycles}"
        )

    return {
        "lane": lane,
        "Sample_ID": sample_id,
        "index": index_seqs[0],
        "BarcodeMismatchesIndex1": 1,
        "index2": index_seqs[1] if len(index_seqs) > 1 else "",
        "BarcodeMismatchesIndex2": 1 if len(index_seqs) > 1 else "",
        "OverrideCycles": override_cycles,
        "Sample_Project": project_id,
    }


def _build_override_cycles(read1_cycles: int, read2_cycles: int, index1_cycles: int, index2_cycles: int, index_cat: str, samplesheet_data: Dict[str, Any]) -> str:
    """
    Build the OverrideCycles string for a sample.

    Args:
        read1_cycles (int): Number of cycles for read 1.
        read2_cycles (int): Number of cycles for read 2.
        index1_cycles (int): Number of cycles for index 1.
        index2_cycles (int): Number of cycles for index 2.
        index_cat (str): Index reagent category.
        samplesheet_data Dict[str, Any]: Overall sample sheet configuration.

    Returns:
        OverrideCycles string (e.g., "Y151;I8;I8;Y151").
    """
    # Build read 1 mask
    if read1_cycles < samplesheet_data["read1_cycles"]:
        read1_mask = f"Y{read1_cycles}N{samplesheet_data['read1_cycles'] - read1_cycles};"
    else:
        read1_mask = f"Y{read1_cycles};"

    # Build read 2 mask
    read2_mask = ""
    if read2_cycles:
        if read2_cycles < samplesheet_data["read2_cycles"]:
            read2_mask = f"Y{read2_cycles}N{samplesheet_data['read2_cycles'] - read2_cycles};"
        else:
            read2_mask = f"Y{read2_cycles};"

    # Build index 1 mask
    if index_cat == "Illumina IDT 384 UMI":
        index1_mask = "I8U9;"
    elif index1_cycles < samplesheet_data["index1_cycles"]:
        index1_mask = f"I{index1_cycles}N{samplesheet_data['index1_cycles'] - index1_cycles};"
    else:
        index1_mask = f"I{index1_cycles};"

    # Build index 2 mask
    index2_mask = ""
    if index2_cycles or samplesheet_data["index2_cycles"]:
        if not index2_cycles:
            index2_mask = f"N{samplesheet_data['index2_cycles']};"
        elif index2_cycles < samplesheet_data["index2_cycles"]:
            index2_mask = f"N{samplesheet_data['index2_cycles'] - index2_cycles}I{index2_cycles};"
        else:
            index2_mask = f"I{index2_cycles};"

    override_cycles = f"{read1_mask}{index1_mask}{index2_mask}{read2_mask}"
    return override_cycles.rstrip(";")


def run(lims: Lims, step_uri: str, output_file: TextIO):
    """
    Execute sample sheet creation and write to file.

    Args:
        lims (Lims): LIMS instance for API interaction.
        step_uri (str): URI of the sequencing step.
        output_file (TextIO): File object to write the sample sheet to.
    """
    samplesheet = create_samplesheet(lims, step_uri)
    output_file.write(samplesheet)
