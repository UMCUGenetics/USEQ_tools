from genologics.entities import Artifact, Step, ProtocolStep
from modules.useq_template import TEMPLATE_PATH,TEMPLATE_ENVIRONMENT,renderTemplate
from io import BytesIO
from openpyxl import load_workbook
from config import Config
import xml.etree.ElementTree as ET
import sys

def parse(lims, step_uri, aid, output_file, mode):
    worksheet_artifact = Artifact(lims, id=aid)
    worksheet_id = worksheet_artifact.files[0].id
    # print(step_uri, aid)
    # sys.exit()
    step = Step(lims, uri=step_uri)
    current_step = step.configuration.name

    barcode_set = None
    if mode == 'illumina':
        barcode_set = Config.UMI_BARCODES
    elif mode == 'ont':
        barcode_set = Config.ONT_BARCODES
    # protocol_step = ProtocolStep(lims, uri=step.configuration.uri)
    # step_fields = []
    # for sf in protocol_step.sample_fields:
    #     if sf['style'] == 'USER_DEFINED':
    #         step_fields.append(sf['name'])


    content = lims.get_file_contents(id=worksheet_id).read()
    wb = load_workbook(filename=BytesIO(content))
    columns = {
        'nr' : None,
        'container name': None,
        'well/tube': None,
        'sample': None,
        'pre conc ng/ul': None,
        'RIN': None,
        'barcode nr': None,
        'post conc ng/ul': None,
        'size': None
    }

    sample_worksheet = wb['Samples']
    for col in sample_worksheet.iter_cols(max_row=1,max_col=sample_worksheet.max_column):
        if col[0].value in columns:
            columns[col[0].value] = col[0].column-1

    #Check if all neccessary columns are set for a step
    if not columns['sample']:
        sys.exit('ERROR : No "sample" column found.')

    samples = {}
    row_nr = 1
    for row_cells in sample_worksheet.iter_rows(min_row=2,max_row=sample_worksheet.max_row):
        sample = {}
        if not row_cells[ columns['sample'] ].value:
            continue
        # sample['sample_name'] = row_cells[ columns['sample'] ].value
        if current_step == 'USEQ - Isolation' or current_step == 'USEQ - Isolation v2':
            if not columns['pre conc ng/ul']:
                sys.exit('ERROR : No "pre conc ng/ul" column found.')
            if row_cells[ columns['pre conc ng/ul'] ].value:
                sample['pre conc ng/ul'] = row_cells[ columns['pre conc ng/ul'] ].value
            else:
                sys.exit(f'ERROR : No "pre conc ng/ul" found at row {row_nr}.')
            if row_cells[ columns['container name'] ].value:
                sample['container name'] = row_cells[ columns['container name'] ].value

        if current_step == 'USEQ - Pre LibPrep QC':
            if not columns['pre conc ng/ul']:
                sys.exit('ERROR : No "pre conc ng/ul" column found.')
            if row_cells[ columns['pre conc ng/ul'] ].value:
                sample['pre conc ng/ul'] = row_cells[ columns['pre conc ng/ul'] ].value
            else:
                sys.exit(f'ERROR : No "pre conc ng/ul" found at row {row_nr}.')

            if columns['RIN'] and row_cells[ columns['RIN'] ].value:
                sample['RIN'] = row_cells[ columns['RIN'] ].value
        if current_step == 'USEQ - LibPrep Illumina' or current_step == 'USEQ - LibPrep Nanopore':
            if not columns['barcode nr']:
                sys.exit('ERROR : No "barcode nr" column found.')
            if row_cells[ columns['barcode nr'] ].value and row_cells[ columns['barcode nr'] ].value in barcode_set:
                sample['barcode'] = barcode_set[ row_cells[ columns['barcode nr'] ].value  ]
                # print(sample['barcode'])
            else:
                sys.exit(f'ERROR : No valid "barcode nr" found at row {row_nr}.')

        if current_step == 'USEQ - Post LibPrep QC':
            if not columns['post conc ng/ul']:
                sys.exit('ERROR : No "post conc ng/ul" column found.')
            if row_cells[ columns['post conc ng/ul'] ].value:
                sample['post conc ng/ul'] = row_cells[ columns['post conc ng/ul'] ].value
            else:
                sys.exit(f'ERROR : No "post conc ng/ul" found at row {row_nr}.')

            if not columns['size']:
                sys.exit('ERROR : No "size" column found.')
            if row_cells[ columns['size'] ].value:
                sample['size'] = row_cells[ columns['size'] ].value
            else:
                sys.exit(f'ERROR : No "size" found at row {row_nr}.')

        if row_cells[ columns['sample'] ].value not in samples:
            samples[ row_cells[ columns['sample'] ].value ] = sample
        row_nr +=1
    artifacts_to_update = []
    containers_to_update = []
    # sys.exit()
    for io_map in step.details.input_output_maps:
        artifact = None
        next_stage = None

        if io_map[1]['output-generation-type'] == 'PerInput':
            artifact = io_map[1]['uri'] #output artifact
            artifact_sample = artifact.samples[0]
            sample_info = samples[ artifact_sample.name ]
            project_id = artifact_sample.project.id

            if 'pre conc ng/ul' in sample_info and not 'Concentration Qubit QC (DNA) 5.0 (ng/ul)' in artifact.udf:
                artifact.udf['Concentration Qubit QC (DNA) 5.0 (ng/ul)'] = float(sample_info['pre conc ng/ul'])
            if 'container name' in sample_info:
                container = artifact.location[0]
                # print(container)
                container.name = sample_info['container name']
                containers_to_update.append(container)
            if 'RIN' in sample_info and not 'RIN' in artifact.udf:
                artifact.udf['RIN'] = float(sample_info['RIN'])
            if 'barcode' in sample_info :

                reagent_label = ET.SubElement(artifact.root, 'reagent-label')
                reagent_label.set('name', sample_info['barcode'])
                output_file.write(f"Barcode added to : {project_id}\t{artifact_sample.name}\t{sample_info['barcode']}\n")

            if 'post conc ng/ul' in sample_info and not 'Concentration Qubit QC (DNA) 5.0 (ng/ul)' in artifact.udf:
                artifact.udf['Concentration Qubit QC (DNA) 5.0 (ng/ul)'] = float(sample_info['post conc ng/ul'])
            if 'size' in sample_info and not 'Average length (bp)' in artifact.udf:
                artifact.udf['Average length (bp)'] = int(sample_info['size'])
            artifacts_to_update.append(artifact)
    lims.put_batch(artifacts_to_update)
    lims.put_batch(containers_to_update)

def run(lims, step_uri, aid, output_file, mode):
	parse(lims, step_uri, aid, output_file, mode)
