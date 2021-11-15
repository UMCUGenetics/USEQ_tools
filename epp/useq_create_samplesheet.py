from genologics.entities import Step, StepDetails, Process
from modules.useq_template import TEMPLATE_PATH,TEMPLATE_ENVIRONMENT,renderTemplate

# [Header]
# Investigator Name,{{investigator_name}}
# Experiment Name,{{experiment_name}}
# Date,{{date}}
# [Reads]
# Read1Cycles,{{read1_cycles}},{% if read2_cycles %}
# Read2Cycles,{{read2_cycles}},{% endif %}
# [Settings]
# OverrideCycles,{{override_cycles}}
# {% if trim_umi %}TrimUMI,0{% endif %}
# CreateFastqForIndexReads,1
# [Data]
# Sample_ID,index,{% if index2_cycles %}index2,{% endif %}Sample_Project
# {% for sample in samples %}{{sample}}
# {% endfor %}


def createSamplesheet(lims, step_uri):
    uri_parts = step_uri.split("/")
    samplesheet_data = {
        'investigator_name' : None,
        'experiment_name' : None,
        'date' : None,
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
    # print(process.type.name)
    # step = Step(lims, uri=step_uri)
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
        # print(a)
        s = {
            'Sample_ID' : a.name,
            'index' : None,
            'index2' : None,
            'Sample_Project' : a.samples[0].project.id
        }
        index_name = a.reagent_labels[0]
        reagent = lims.get_reagent_types(name=index_name)[0]
        # print(reagent.sequence)
        if '-' in reagent.sequence:
            s['index'],s['index2'] = reagent.sequence.split('-')
            samplesheet_data['index1_cycles'] = len(s['index'])
            samplesheet_data['index2_cycles'] = len(s['index2'])
        else:
            s['index'] = reagent.sequence
            samplesheet_data['index1_cycles'] = len(s['index'])

        samplesheet_data['samples'].append(s)
    # print(samplesheet_data['index1_cycles'],samplesheet_data['index2_cycles'])
    samplesheet_data['override_cycles'] = f'Y{ samplesheet_data["read1_cycles"] };'
    if samplesheet_data['index1_cycles'] == 17:
        if process.type.name in ['NextSeq2000','NovaSeq']:
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
    # for io in pooling_process
    # artifacts = pooling_process.all_inputs()
    # for a in artifacts:
    #     # print(a.reagent_labels)


def run(lims, step_uri, output_file):
    """Runs the createSamplesheet function"""
    recipe = createSamplesheet(lims, step_uri)

    output_file.write(recipe)
