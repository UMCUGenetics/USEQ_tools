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
        'dual_index' : False,
        'trim_umi' : None,
        'samples' : [
            # 'Sample_ID' :
            # 'index' :
            # 'index2' :
            # 'Sample_Project' :
            # 'OverrideCycles' :
            # 'BarcodeMismatchesIndex1'
            # 'BarcodeMismatchesIndex2'
        ]
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

        index_name = a.reagent_labels[0]

        reagent = lims.get_reagent_types(name=index_name)[0]
        index_cat = reagent.category
        sample = a.samples[0]
        index_seqs = [index for index in reagent.sequence.split('-')]

        read1_cycles = f"Y{samplesheet_data['read1_cycles']};"
        read2_cycles = f"Y{samplesheet_data['read2_cycles']};"
        index1_cycles = 'I8U9;' if index_cat == 'Illumina IDT 384 UMI' else f"I{len(index_seqs[0])};"
        index2_cycles = ''

        if len(index_seqs) > 1:
            index2_len = len(index_seqs[1])
            index2_cycles = f"I{index2_len};"
            samplesheet_data['dual_index'] = True

        if 'USEQ Read1 Cycles' in sample.udf:
            read1_cycles = f"Y{int(sample.udf['USEQ Read1 Cycles']) + 1};"

        if 'USEQ Read2 Cycles' in sample.udf:
            read2_cycles = f"Y{int(sample.udf['USEQ Read2 Cycles']) + 1}"

        override_cycles = f'{read1_cycles}{index1_cycles}{index2_cycles}{read2_cycles}'

        s = {
            'Sample_ID' : sample.name,
            'index' : index_seqs[0],
            'index2' : index_seqs[1] if index2_cycles else None,
            'Sample_Project' : sample.project.id,
            'OverrideCycles' : override_cycles,
            'BarcodeMismatchesIndex1' : 1, #set default to 1
            'BarcodeMismatchesIndex2' : 1 if index2_cycles else None, #set default to 1
        }


        samplesheet_data['samples'].append(s)

    samplesheet = renderTemplate('SampleSheetv2_template.csv', samplesheet_data)
    return samplesheet

def createSamplesheetNovaseqX(lims, step_uri):
    uri_parts = step_uri.split("/")
    samplesheet_data = {
        'investigator_name' : None,
        'experiment_name' : None,
        'date' : None,
        'lanes' : True,
        'read1_cycles' : '',
        'read2_cycles' : '',
        'dual_index' : False,
        'trim_umi' : None,
        'samples' : [
            # 'Sample_ID' :
            # 'index' :
            # 'index2' :
            # 'Sample_Project' :
            # 'OverrideCycles' :
            # 'BarcodeMismatchesIndex1'
            # 'BarcodeMismatchesIndex2'
        ]
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
        #Grab the index sequence from the input artifacts in the pooling process.
        #Note for future Sander: Pooling process can also contain samples not included in the current run, this is why we go over the samples twice
        for io in pooling_process.input_output_maps:
            a = io[0]['uri']

            sample_name = a.samples[0].name
            if sample_name in sample_demux : continue
            sample_demux[sample_name]= {}
            index_name = a.reagent_labels[0]

            reagent = lims.get_reagent_types(name=index_name)[0]

            sample_demux[sample_name]['index_seq'] = [index for index in reagent.sequence.split('-')]
            sample_demux[sample_name]['index_cat'] = reagent.category

        samples = sample_pool.samples
        project_id = samples[0].project.id
        nr_lanes = samples[0].udf.get('Number Lanes', 8)

        for nr in range(nr_lanes):
            lane = []
            for sample in sample_pool.samples:

                read1_cycles = f"Y{samplesheet_data['read1_cycles']};"
                read2_cycles = f"Y{samplesheet_data['read2_cycles']};"
                index1_cycles = 'I8U9;' if sample_demux[sample.name]['index_cat'] == 'Illumina IDT 384 UMI' else f"I{len(sample_demux[sample.name]['index_seq'][0])};"
                index2_cycles = ''


                if len(sample_demux[sample.name]['index_seq']) > 1:
                    index2_len = len(sample_demux[sample.name]['index_seq'][1])
                    index2_cycles = f"I{index2_len};"
                    samplesheet_data['dual_index'] = True

                if 'USEQ Read1 Cycles' in sample.udf:
                    read1_cycles = f"Y{int(sample.udf['USEQ Read1 Cycles']) + 1};"

                if 'USEQ Read2 Cycles' in sample.udf:
                    read2_cycles = f"Y{int(sample.udf['USEQ Read2 Cycles']) + 1}"

                override_cycles = f'{read1_cycles}{index1_cycles}{index2_cycles}{read2_cycles}'

                s = {
                    'Sample_ID' : sample.name,
                    'index' : sample_demux[sample.name]['index_seq'][0],
                    'BarcodeMismatchesIndex1' : 1, #set default to 1
                    'index2' : sample_demux[sample.name]['index_seq'][1] if index2_cycles else None,
                    'BarcodeMismatchesIndex2' : 1 if index2_cycles else None, #set default to 1
                    'OverrideCycles' : override_cycles,
                    'Sample_Project' : project_id
                }
                lane.append(s)
            samplesheet_data['samples'].append(lane)


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
