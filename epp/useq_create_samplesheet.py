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


    pool = process.input_output_maps[0][0]['uri']

    pooling_process = pool.parent_process
    for io in pooling_process.input_output_maps:
        if io[1]['limsid'] != pool.id: continue
        a = io[0]['uri']

        index_name = a.reagent_labels[0]
        # print(a.name)
        reagent = lims.get_reagent_types(name=index_name)[0]
        index_cat = reagent.category
        sample = a.samples[0]
        index_seqs = [index for index in reagent.sequence.split('-')]

        read1_cycles = f"Y{samplesheet_data['read1_cycles']};"
        read2_cycles = f"Y{samplesheet_data['read2_cycles']}" if samplesheet_data['read2_cycles'] else ''
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
        override_cycles = override_cycles.rstrip(";")
        s = {
            'Sample_ID' : a.name,
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
        'index1_cycles' : '',
        'index2_cycles' : '',
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
    io_maps = process.input_output_maps
    io_maps_filtered = [iom for iom in io_maps if iom[1]['output-type'] == 'Analyte']

    technician = process.technician
    samplesheet_data['investigator_name'] = f"{technician.first_name} {technician.last_name}"
    samplesheet_data['experiment_name'] = process.udf['Experiment Name']
    samplesheet_data['date'] = process.date_run
    samplesheet_data['read1_cycles'] = process.udf['Read 1 Cycles']
    if 'Read 2 Cycles' in process.udf : samplesheet_data['read2_cycles'] = process.udf['Read 2 Cycles']
    samplesheet_data['index1_cycles'] = process.udf['Index Read 1']
    if 'Index Read 2' in process.udf : samplesheet_data['index2_cycles'] = process.udf['Index Read 2']

    current_lane = 1
    for io_map in io_maps_filtered:
        #Every io_map contains one pool of samples (projectID)
        input_artifact, output_artifact = io_map

        sample_pool = input_artifact['uri']
        samples = [x.name for x in sample_pool.samples]
        pooling_process = sample_pool.parent_process
        project_id = None
        temp_samples = []
        nr_lanes = 1 #Decent default
        #Grab the index sequence from the input artifacts in the pooling process.
        #Note for future Sander: Pooling process can also contain samples not included in the current run, this is why we go over the samples twice
        for io in pooling_process.input_output_maps:
            a = io[0]['uri']

            sample = a.samples[0]
            artifact_name = a.name
            if sample.name not in samples: continue
            # print(artifact_name)
            index_name = a.reagent_labels[0]
            reagent = lims.get_reagent_types(name=index_name)[0]
            index_seqs = [index for index in reagent.sequence.split('-')]
            index_cat = reagent.category
            project_id = sample.project.id

            if 'Number Lanes' in sample.udf:
                nr_lanes = sample.udf.get('Number Lanes')
                if len(io_maps_filtered) > 1 and nr_lanes == 8: #Can't have 8 lanes for a pool when run contains >1 project
                    nr_lanes = 1

            elif len(io_maps_filtered) == 1: #Only one project on one flowcell
                nr_lanes = 8

            # print(nr_lanes)
            read1_cycles = f"Y{samplesheet_data['read1_cycles']};"
            read2_cycles = f"Y{samplesheet_data['read2_cycles']};" if samplesheet_data['read2_cycles'] else ''
            index1_cycles = f"I{samplesheet_data['index1_cycles']};"
            if index_cat == 'Illumina IDT 384 UMI':
                index1_cycles = 'I8U9;'
            index2_cycles = f"I{samplesheet_data['index2_cycles']};" if samplesheet_data['index2_cycles'] else ''

            if 'USEQ Read1 Cycles' in sample.udf:
                r1_c = int(sample.udf['USEQ Read1 Cycles']) + 1
                if r1_c < samplesheet_data['read1_cycles']:
                    read1_cycles = f"Y{r1_c}N{ samplesheet_data['read1_cycles'] - r1_c};"
                else:
                    read1_cycles = f"Y{r1_c};"

            if 'USEQ Read2 Cycles' in sample.udf:
                r2_c = int(sample.udf['USEQ Read2 Cycles']) + 1
                if r2_c < samplesheet_data['read2_cycles']:
                    read2_cycles = f"Y{r2_c}N{ samplesheet_data['read2_cycles'] - r2_c};"
                else:
                    read2_cycles = f"Y{r2_c};"

            if index_seqs[0]:
                i1_c = len(index_seqs[0])
                if index_cat == 'Illumina IDT 384 UMI':
                    index1_cycles = 'I8U9;'
                elif i1_c < samplesheet_data['index1_cycles']:
                    index1_cycles = f"I{i1_c}N{samplesheet_data['index1_cycles'] - i1_c};"
                else:
                    index1_cycles = f"I{i1_c};"

            if len(index_seqs) > 1:
                samplesheet_data['dual_index'] = True
                i2_c = len(index_seqs[1])
                if i2_c < samplesheet_data['index2_cycles']:
                    index2_cycles = f"N{samplesheet_data['index2_cycles'] - i2_c}I{i2_c};"
                else:
                    index2_cycles = f"I{i2_c};"

            override_cycles = f'{read1_cycles}{index1_cycles}{index2_cycles}{read2_cycles}'
            override_cycles = override_cycles.rstrip(";")

            s = {
                'Sample_ID' : artifact_name,
                'index' : index_seqs[0],
                'BarcodeMismatchesIndex1' : 1, #set default to 1
                'index2' : index_seqs[1] if index2_cycles else None,
                'BarcodeMismatchesIndex2' : 1 if index2_cycles else None, #set default to 1
                'OverrideCycles' : override_cycles,
                'Sample_Project' : project_id
            }
            temp_samples.append(s)
            # print(project_id, override_cycles)
            # for nr in range(current_lane,current_lane+nr_lanes):
            #     print(nr)
            #     current_lane += 1

            print(project_id, index_seqs)
        for l in range(nr_lanes):
            samplesheet_data['samples'].append(temp_samples)

        # print(samplesheet_data)


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
