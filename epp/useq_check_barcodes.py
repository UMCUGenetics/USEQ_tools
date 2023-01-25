from genologics.entities import Artifact, Step, ProtocolStep,Process
from modules.useq_template import TEMPLATE_PATH,TEMPLATE_ENVIRONMENT,renderTemplate
from config import Config
import sys

def check_barcodes(lims, step_uri):
    process = Process(lims, id=step_uri.split("/")[-1])

    for artifact in process.analytes()[0]:
        sample = artifact.samples[0]

        if not artifact.reagent_labels:
            print(f'{sample.name} has no barcode')
            exit(-1)

def run(lims, step_uri):
	check_barcodes(lims, step_uri)
