from genologics.entities import Step, StepDetails, Process
from modules.useq_template import TEMPLATE_PATH,TEMPLATE_ENVIRONMENT,renderTemplate
from config import Config
import sys

def createSamplesheet(lims, step_uri):
    uri_parts = step_uri.split("/")
    samplesheet_data = {
        'investigator_name' : None,
        'experiment_name' : None,
        'date' : None,
        'lanes' : False,
        'read1_cycles' : 0,
        'read2_cycles' : 0,
        'index1_cycles' : 0,
        'index2_cycles' : 0,
        'dual_index' : False,
        'trim_umi' : None,
        'samples' : [

        ]
    }
    process_id = uri_parts[-1]

    process = Process(lims, id=process_id)
    seq_io_maps = process.input_output_maps
    seq_io_maps_filtered = [iom for iom in seq_io_maps if iom[1]['output-type'] == 'Analyte']
    if Config.DEVMODE: print(f'Processing {len(seq_io_maps_filtered)} input-output maps')

    technician = process.technician
    samplesheet_data['investigator_name'] = f"{technician.first_name} {technician.last_name}"
    samplesheet_data['experiment_name'] = process.udf['Experiment Name']
    samplesheet_data['date'] = process.date_run
    samplesheet_data['read1_cycles'] = process.udf['Read 1 Cycles']
    if 'Read 2 Cycles' in process.udf :
        samplesheet_data['read2_cycles'] = process.udf['Read 2 Cycles']
    samplesheet_data['index1_cycles'] = int(process.udf.get('Index Read 1', 0))
    if 'Index Read 2' in process.udf :
        samplesheet_data['index2_cycles'] = int(process.udf.get('Index Read 2', 0))

    if Config.DEVMODE:
        print('Samplesheet header has following settings:')
        print(f"Investigator {samplesheet_data['investigator_name']}")
        print(f"Experiment Name {samplesheet_data['experiment_name']}")
        print(f"Date {samplesheet_data['date']}")
        print(f"Default Read Settings : {samplesheet_data['read1_cycles']};{samplesheet_data['index1_cycles'] };{samplesheet_data['index2_cycles']};{samplesheet_data['read2_cycles']};")

    for seq_io_map in seq_io_maps_filtered:
        #Every io_map contains one pool of samples
        input_pool, lane_pool = [io['uri'] for io in seq_io_map] #Grab the input & output artifact objects by their uri


        lane_placements = dict((v.id,k) for k,v in lane_pool.container.placements.items()) #{artifact1ID : 1:1, artifact2ID : 2:1,}
        if len(lane_placements) > 1:
            samplesheet_data['lanes'] = True
        # input_pool_samples = [x.name for x in input_pool.samples]
        # print(input_pool_samples)

        pooling_process = input_pool.parent_process
        project_id = None

        #Grab the index sequence from the input artifacts in the pooling process.
        #Note for future Sander: Pooling process can also contain samples not included in the current run, this is why we go over the samples twice
        for pooling_io_map in pooling_process.input_output_maps:
            #One io_map per original samples in the pooling process
            input_sample_artifact, output_pool = [io['uri'] for io in pooling_io_map] #Grab the input & output artifact objects by their uri
            # print(input_sample_artifact.name)
            input_sample = input_sample_artifact.samples[0] #Artifact only contains one sample
            # if input_sample.name not in input_pool_samples: continue #Skip samples that were included in the pooling step, but not in the sequencing step.
            # print(input_sample.udf['Sequencing Runtype'])
            index_name = input_sample_artifact.reagent_labels[0]
            reagent = lims.get_reagent_types(name=index_name)[0]
            index_seqs = [index for index in reagent.sequence.split('-')]
            index_cat = reagent.category
            project_id = input_sample.project.id

            #Set cycle defaults for sample
            read1_cycles = samplesheet_data['read1_cycles']
            read2_cycles = samplesheet_data['read2_cycles'] if samplesheet_data['read2_cycles'] else 0
            index1_cycles = samplesheet_data['index1_cycles']
            index2_cycles = samplesheet_data['index2_cycles'] if samplesheet_data['index2_cycles'] else 0
            read1_mask = ''
            read2_mask = ''
            index1_mask = ''
            index2_mask = ''
            override_cycles = ''

            #Changes cycles from defaults where needed
            if 'USEQ Read1 Cycles' in input_sample.udf:
                r1_customer = int(input_sample.udf['USEQ Read1 Cycles'])
                if abs(r1_customer - read1_cycles) > 1: #1 base differences, in this case the seq operator is usually right
                    read1_cycles = r1_customer

            if 'USEQ Read2 Cycles' in input_sample.udf:
                r2_customer = int(input_sample.udf['USEQ Read2 Cycles'])
                if abs(r2_customer - read2_cycles) > 1: #1 base differences, in this case the seq operator is usually right
                    read2_cycles = r2_customer

            if len(index_seqs) == 2: #Dual index
                samplesheet_data['dual_index'] = True
                index1_cycles = len(index_seqs[0])
                index2_cycles = len(index_seqs[1])
            else:
                index1_cycles = len(index_seqs[0])
                index2_cycles = 0

            if 'Read Settings' in lane_pool.udf and lane_pool.udf['Read Settings']:
                #Read Settings overrides all other preferences
                read1_cycles,index1_cycles,index2_cycles,read2_cycles = [int(x) for x in lane_pool.udf['Read Settings'].split(";")] #r1,i1,i2,r2

            #Set read/index masks
            if read1_cycles < samplesheet_data['read1_cycles']:
                read1_mask = f"Y{read1_cycles}N{ samplesheet_data['read1_cycles'] - read1_cycles};"
            else:
                read1_mask = f"Y{read1_cycles};"

            if read2_cycles :
                if read2_cycles < samplesheet_data['read2_cycles']:
                    read2_mask = f"Y{read2_cycles}N{ samplesheet_data['read2_cycles'] - read2_cycles};"
                else:
                    read2_mask = f"Y{read2_cycles};"

            if index_cat == 'Illumina IDT 384 UMI':
                index1_mask = 'I8U9;'
            elif index1_cycles < samplesheet_data['index1_cycles']:
                index1_mask = f"I{index1_cycles}N{samplesheet_data['index1_cycles'] - index1_cycles};"
            else:
                index1_mask = f"I{index1_cycles};"


            if index2_cycles or samplesheet_data['index2_cycles']:
                if not index2_cycles:
                    index2_mask = f"N{samplesheet_data['index2_cycles']};"
                elif index2_cycles < samplesheet_data['index2_cycles']:
                    index2_mask = f"N{samplesheet_data['index2_cycles'] - index2_cycles}I{index2_cycles};"
                else:
                    index2_mask = f"I{index2_cycles};"

            override_cycles = f'{read1_mask}{index1_mask}{index2_mask}{read2_mask}'
            override_cycles = override_cycles.rstrip(";")
            # if Config.DEVMODE: print(f"Processing sample {input_sample.name} with index {index_seqs} for projectID {project_id} on lane {lane_placements[lane_pool.id]} with settings {override_cycles}")
            # sample_id = input_sample.name
            # if input_sample.udf['Sequencing Runtype'] == '60 SNP NimaGen panel':
            sample_id = input_sample_artifact.name
            if Config.DEVMODE: print(f"Processing sample {sample_id} with index {index_seqs} for projectID {project_id} on lane {lane_placements[lane_pool.id]} with settings {override_cycles}")

            sample = {
                'lane' : lane_placements[lane_pool.id].split(":")[0],
                'Sample_ID' : sample_id,
                'index' : index_seqs[0],
                'BarcodeMismatchesIndex1' : 1, #set default to 1
                'index2' : index_seqs[1] if len(index_seqs) > 1 else '',
                'BarcodeMismatchesIndex2' : 1 if len(index_seqs) > 1 else '', #set default to 1
                'OverrideCycles' : override_cycles,
                'Sample_Project' : project_id
            }
            samplesheet_data['samples'].append(sample)


    samplesheet = renderTemplate('SampleSheetv2_template.csv', samplesheet_data)
    return samplesheet



def run(lims, step_uri, output_file, type):
    """Runs the createSamplesheet function"""

    recipe = None
    recipe = createSamplesheet(lims, step_uri)
    output_file.write(recipe)
