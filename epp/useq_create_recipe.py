from genologics.entities import Step, StepDetails
from modules.useq_template import TEMPLATE_PATH,TEMPLATE_ENVIRONMENT,renderTemplate

def createRecipe(lims, step_uri):
    # step = Step(lims, uri=step_uri)
    step_details = StepDetails(lims, uri=step_uri+'/details')

    experiment_name = step_details.udf['Experiment Name']
    flowcell_ID = step_details.udf['Flow Cell ID']
    run_mode = step_details.udf['Run Mode']
    workflow_type = step_details.udf['Workflow Type']
    sample_loading_type = 'NovaSeqStandard'
    paired_end = step_details.udf['Paired End']
    read1 = step_details.udf['Read 1 Cycles']
    read2 = step_details.udf['Read 2 Cycles']
    index_read1 = step_details.udf['Index Read 1']
    index_read2 = step_details.udf['Index Read 2']
    novaseq_output_folder = '\\\\\\\\hodor\\\\results_novaseq_umc01\\\\'
    use_custom_read1_primer = step_details.udf['Use Custom Read 1 Primer']
    use_custom_read2_primer = step_details.udf['Use Custom Read 2 Primer']
    use_custom_index_read1_primer = step_details.udf['Use Custom Index Read 1 Primer']

    librarytube_ID = None
    for input_output in step_details.input_output_maps:
        if input_output[1]['output-generation-type'] == 'PerInput': #only in step output analyte
          artifact = input_output[1]['uri']
          container = artifact.location[0]
          librarytube_ID = container.name
          break

    attachment = f"{novaseq_output_folder}samplesheet\\\\{librarytube_ID}\\\\SampleSheet.csv"

    run_name = f"Novaseq_USEQ_{experiment_name}_{flowcell_ID}"

    recipe = renderTemplate('recipe_template.json', {
        'run_name' : run_name,
        'run_mode' : run_mode,
        'workflow_type' : workflow_type,
        'librarytube_ID' : librarytube_ID,
        'flowcell_ID' : flowcell_ID,
        'rehyb' : 'false',
        'paired_end' : paired_end,
    	'read1':read1,
    	'read2':read2,
    	'index_read1':index_read1,
    	'index_read2':index_read2,
    	'output_folder':novaseq_output_folder,
    	'attachment':attachment,
    	'use_basespace':'false',
    	'basespace_mode':'null',
    	'use_custom_read1_primer':use_custom_read1_primer,
    	'use_custom_read2_primer':use_custom_read2_primer,
    	'use_custom_index_read1_primer':use_custom_index_read1_primer
    })

    return recipe


def run(lims, step_uri, output_file):
    """Runs the createRecipe function"""
    recipe = createRecipe(lims, step_uri)

    output_file.write(recipe)
# {
# 	"run_name":"{{run_name}}",
# 	"run_mode":"{{run_mode}}",
# 	"workflow_type":"{{workflow_type}}",
# 	"sample_loading_type":"NovaSeqStandard",
# 	"librarytube_ID":"{{librarytube_ID}}",
# 	"flowcell_ID":"{{flowcell_ID}}",
# 	"rehyb":{{rehyb}},
# 	"paired_end":{{paired_end}},
# 	"read1":{{read1}},
# 	"read2":{{read2}},
# 	"index_read1":{{index_read1}},
# 	"index_read2":{{index_read2}},
# 	"output_folder":"{{output_folder}}",
# 	"attachment":"{{attachment}}",
# 	"use_basespace":{{use_basespace}},
# 	"basespace_mode":{{basespace_mode}},
# 	"use_custom_read1_primer":{{use_custom_read1_primer}},
# 	"use_custom_read2_primer":{{use_custom_read2_primer}},
# 	"use_custom_index_read1_primer":{{use_custom_index_read1_primer}}
# }

# <udf:field type="String" name="BaseSpace Sequence Hub Configuration">Not Used</udf:field>
# <udf:field type="String" name="Experiment Name">Novaseq_USEQ_CON4873_HMYHCDRXX</udf:field>
# <udf:field type="Boolean" name="Use Custom Index Read 1 Primer">false</udf:field>
# <udf:field type="Boolean" name="Use Custom Read 1 Primer">false</udf:field>
# <udf:field type="Boolean" name="Use Custom Read 2 Primer">false</udf:field>
# <udf:field type="String" name="Run Mode">S1</udf:field>
# <udf:field type="String" name="Sample Sheet Path">
# \\fijnspar\results_novaseq_umc01\samplesheet\NV0229237-LIB\SampleSheet.csv
# </udf:field>
# <udf:field type="String" name="Workflow Type">Dual Index</udf:field>
# <udf:field type="String" name="Paired End">False</udf:field>
# <udf:field type="Numeric" name="Read 1 Cycles">151</udf:field>
# <udf:field type="Numeric" name="Read 2 Cycles">151</udf:field>
# <udf:field type="Numeric" name="Index Read 1">8</udf:field>
# <udf:field type="Numeric" name="Index Read 2">8</udf:field>
# <udf:field type="String" name="Flow Cell ID">HMYHCDRXX</udf:field>
