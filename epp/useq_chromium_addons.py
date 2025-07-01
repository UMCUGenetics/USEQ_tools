from genologics.entities import Artifact, Step, ProtocolStep
from config import Config
import sys

def rename_samples(lims, step_uri):
    step = Step(lims, uri=step_uri)


    add_ons = ['']
    artifacts_to_update = {}
    nr_derived_samples = 0
    nr_input_samples = 0
    if step.details.udf['BCR']: add_ons.append('BCR')
    if step.details.udf['TCR']: add_ons.append('TCR')
    if step.details.udf['Cell surface protein']: add_ons.append('CSP')
    if step.details.udf['CRISPR']: add_ons.append('CRISPR')

    for io_map in step.details.input_output_maps:
        input_artifact,output_artifact = io_map

        if output_artifact['output-generation-type'] == 'PerInput':
            if input_artifact['uri'] not in artifacts_to_update:
                artifacts_to_update[ input_artifact['uri'] ] = []
                nr_input_samples += 1
            nr_derived_samples += 1
            artifacts_to_update[input_artifact['uri']].append( output_artifact['uri'] )

    if len(add_ons) != nr_derived_samples/nr_input_samples :
        sys.exit(f'The number of derived samples (per input sample) is not equal to the number of add-ons ({len(add_ons)})')

    artifact_batch = []
    for ia in artifacts_to_update:
        for idx,oa in enumerate(artifacts_to_update[ia]):
            if not add_ons[idx]:
                oa.name = f"{ia.name}"
            else:
                oa.name = f"{ia.name}-{add_ons[idx]}"
            print(oa.name)
            artifact_batch.append(oa)

    lims.put_batch(artifact_batch)
            # print(ia.name, add_ons[idx])
def run(lims,step_uri):
    rename_samples(lims,step_uri)


 # https://usf-lims.umcutrecht.nl/api/v2/steps/24-617927
