from config import Config
from genologics.entities import Step
from genologics.lims import Lims
import sys

def updateUDF(lims, step_uri, udf_name, udf_value):
    step = Step(lims, uri=step_uri)

    if udf_name not in step.details.udf:
        sys.exit(f'{udf_name} does not exist in step')
    else:
        step.details.udf[udf_name] = udf_value
        step.details.put()


def run(lims, step_uri, udf_name, udf_value):

    updateUDF(lims, step_uri, udf_name, udf_value)
