"""Module for updating UDF values in LIMS steps."""

import sys
from typing import Any

from genologics.entities import Step
from genologics.lims import Lims

from config import Config


def update_udf(lims: Lims, step_uri: str, udf_name: str, udf_value: Any) -> None:
    """
    Update a UDF (User-Defined Field) value for a given step.

    Args:
        lims (Lims): LIMS instance
        step_uri (str): URI of the step to update.
        udf_name (str): Name of the UDF to update.
        udf_value (Any): New value to set for the UDF.

    Raises:
        SystemExit: If the specified UDF does not exist in the step.
    """
    step = Step(lims, uri=step_uri)

    if udf_name not in step.details.udf:
        sys.exit(f"'{udf_name}' does not exist in step")

    step.details.udf[udf_name] = udf_value
    step.details.put()


def run(lims: Lims, step_uri: str, udf_name: str, udf_value: Any) -> None:
    """
    Execute UDF update operation.

    Args:
        lims (Lims): LIMS instance
        step_uri (str): URI of the step to update.
        udf_name (str): Name of the UDF to update.
        udf_value (Any): New value to set for the UDF.
    """
    update_udf(lims, step_uri, udf_name, udf_value)
