from genologics.entities import Step, StepDetails, Process
from modules.useq_template import TEMPLATE_PATH,TEMPLATE_ENVIRONMENT,renderTemplate
import sys


def createSamplesheet(lims, step_uri):
    uri_parts = step_uri.split("/")
    samplesheet_data = {
        'investigator_name' : None,
        'experiment_name' : None,
        'date' : None,
        'lanes':False,
        'read1_cycles' : None,
        'read2_cycles' : None,
        'index1_cycles' : None,
        'index2_cycles' : None,
        'override_cycles' : None,
        'trim_umi' : None,
        'samples' : []
    }
    process_id = uri_parts[-1]

    process = Process(lims, id=process_id)

    technician = process.technician
    samplesheet_data['investigator_name'] = f"{technician.first_name} {technician.last_name}"
    samplesheet_data['experiment_name'] = process.udf['Experiment Name']
    samplesheet_data['date'] = process.date_run
    samplesheet_data['read1_cycles'] = process.udf['Read 1 Cycles']
    if 'Read 2 Cycles' in process.udf : samplesheet_data['read2_cycles'] = process.udf['Read 2 Cycles']


    pool = process.input_output_maps[0][0]['uri']

    pooling_process = pool.parent_process
    for io in pooling_process.input_output_maps:
        if io[1]['limsid'] != pool.id: continue
        a = io[0]['uri']

        s = {
            'Sample_ID' : a.name,
            'index' : None,
            'index2' : None,
            'Sample_Project' : a.samples[0].project.id
        }
        index_name = a.reagent_labels[0]
        reagent = lims.get_reagent_types(name=index_name)[0]

        if '-' in reagent.sequence:
            s['index'],s['index2'] = reagent.sequence.split('-')
            samplesheet_data['index1_cycles'] = len(s['index'])
            samplesheet_data['index2_cycles'] = len(s['index2'])
        else:
            s['index'] = reagent.sequence
            samplesheet_data['index1_cycles'] = len(s['index'])

        samplesheet_data['samples'].append(s)

    samplesheet_data['override_cycles'] = f'Y{ samplesheet_data["read1_cycles"] };'

    if samplesheet_data['index1_cycles'] == 17:
        if 'NextSeq2000' in process.type.name or 'NovaSeq' in process.type.name:
            samplesheet_data['override_cycles'] += 'I8U9;'
            samplesheet_data['trim_umi'] = True
        else:
            samplesheet_data['override_cycles'] += 'I8;'
    else:
        samplesheet_data['override_cycles'] += f'I{ samplesheet_data["index1_cycles"] };'
    if samplesheet_data['index2_cycles']:
        samplesheet_data['override_cycles'] += f'I{ samplesheet_data["index2_cycles"] };'
    if samplesheet_data['read2_cycles']:
        samplesheet_data['override_cycles'] += f'Y{ samplesheet_data["read2_cycles"] };'

    samplesheet_data['override_cycles'] = samplesheet_data['override_cycles'][:-1]
    samplesheet = renderTemplate('SampleSheetv2_template.csv', samplesheet_data)
    return samplesheet

def createSamplesheetNovaseqX(lims, step_uri):
    uri_parts = step_uri.split("/")
    samplesheet_data = {
        'investigator_name' : None,
        'experiment_name' : None,
        'date' : None,
        'lanes' : True,
        'read1_cycles' : None,
        'read2_cycles' : None,
        'index1_cycles' : None,
        'index2_cycles' : None,
        'override_cycles' : None,
        'trim_umi' : None,
        'samples' : []
    }
    process_id = uri_parts[-1]

    process = Process(lims, id=process_id)

    technician = process.technician
    samplesheet_data['investigator_name'] = f"{technician.first_name} {technician.last_name}"
    samplesheet_data['experiment_name'] = process.udf['Experiment Name']
    samplesheet_data['date'] = process.date_run
    samplesheet_data['read1_cycles'] = process.udf['Read 1 Cycles']
    if 'Read 2 Cycles' in process.udf : samplesheet_data['read2_cycles'] = process.udf['Read 2 Cycles']

    for io_map in process.input_output_maps:
        input_artifact, output_artifact = io_map
        if output_artifact['output-type'] != 'Analyte' : continue #Skip resultfiles

        sample_pool = input_artifact['uri']
        pooling_process = sample_pool.parent_process
        sample_demux = {}
        for io in pooling_process.input_output_maps:
            a = io[0]['uri']
            sample_name = a.samples[0].name
            if sample_name in sample_demux : continue

            index_name = a.reagent_labels[0]
            reagent = lims.get_reagent_types(name=index_name)[0]
            index_seqs = (None,None)
            if '-' in reagent.sequence:
                index_seqs = reagent.sequence.split('-')
                samplesheet_data['index1_cycles'] = len(index_seqs[0])
                samplesheet_data['index2_cycles'] = len(index_seqs[1])
            else:
                index_seqs = reagent.sequence, None
                samplesheet_data['index1_cycles'] = len(index_seqs[0])

            sample_demux[sample_name] = index_seqs

        samples = sample_pool.samples
        project_id = samples[0].project.id
        nr_lanes = samples[0].udf.get('Number Lanes', 8)
        # print(f'Number lanes : {nr_lanes}')
        for nr in range(nr_lanes):
            lane = []
            for sample in sample_pool.samples:
                s = {
                    'Sample_ID' : sample.name,
                    'index' : sample_demux[sample.name][0],
                    'index2' : sample_demux[sample.name][1],
                    'Sample_Project' : project_id
                }
                lane.append(s)
            samplesheet_data['samples'].append(lane)
    ####################################
    samplesheet_data['override_cycles'] = f'Y{ samplesheet_data["read1_cycles"] };'

    if samplesheet_data['index1_cycles'] == 17:
        if 'NextSeq2000' in process.type.name or 'NovaSeq' in process.type.name:
            samplesheet_data['override_cycles'] += 'I8U9;'
            samplesheet_data['trim_umi'] = True
        else:
            samplesheet_data['override_cycles'] += 'I8;'
    else:
        samplesheet_data['override_cycles'] += f'I{ samplesheet_data["index1_cycles"] };'
    if samplesheet_data['index2_cycles']:
        samplesheet_data['override_cycles'] += f'I{ samplesheet_data["index2_cycles"] };'
    if samplesheet_data['read2_cycles']:
        samplesheet_data['override_cycles'] += f'Y{ samplesheet_data["read2_cycles"] };'

    samplesheet_data['override_cycles'] = samplesheet_data['override_cycles'][:-1]

    ######################################
    samplesheet = renderTemplate('SampleSheetv2_template.csv', samplesheet_data)
    return samplesheet


def run(lims, step_uri, output_file, type):
    """Runs the createSamplesheet function"""

    recipe = None
    if type == 'NovaseqX':
        recipe = createSamplesheetNovaseqX(lims, step_uri)
    else:
        recipe = createSamplesheet(lims, step_uri)

    output_file.write(recipe)
