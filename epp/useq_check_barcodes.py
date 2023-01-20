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
            # print (sample,artifact.reagent_labels[0])

    # step = Step(lims, uri=step_uri)
    # for io_map in step.details.input_output_maps:
    #
    #     artifact = None
    #     next_stage = None
    #
    #     if io_map[1]['output-generation-type'] == 'PerInput':
    #         artifact = io_map[1]['uri'] #output artifact
    #         print(io_map[0]['uri'],artifact)

def run(lims, step_uri):
	check_barcodes(lims, step_uri)


# https://usf-lims.umcutrecht.nl/api/v2/steps/24-409095
