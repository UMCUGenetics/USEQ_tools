"""Module for validating barcode assignment in LIMS processes."""

import sys
from typing import List

from genologics.entities import Artifact, Process, Sample
from genologics.lims import Lims


def check_barcodes(lims: Lims, step_uri: str):
    """Check that all artifacts in a process have assigned barcodes.

    Validates that each artifact in the process has a reagent label (barcode)
    assigned. If any artifact is missing a barcode, prints an error message
    and exits with status code -1.

    Args:
        lims (Lims): LIMS instance
        step_uri (str): URI of the processing step

    Raises:
        SystemExit: If any artifact is missing a barcode assignment
    """
    process_id = step_uri.split("/")[-1]
    process = Process(lims, id=process_id)

    analytes = process.analytes()[0]

    for artifact in analytes:
        sample = artifact.samples[0]

        if not artifact.reagent_labels:
            print(f'{sample.name} has no barcode')
            sys.exit(-1)


def run(lims: Lims, step_uri: str):
    """Entry point for the barcode validation script.

    Args:
        lims (Lims): LIMS instance
        step_uri (str): URI of the processing step

    Raises:
        SystemExit: If any artifact is missing a barcode assignment
    """
    check_barcodes(lims, step_uri)
