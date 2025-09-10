from genologics.entities import Step, ProtocolStep
from modules.useq_template import TEMPLATE_PATH,TEMPLATE_ENVIRONMENT,renderTemplate
from config import Config
import re
import sys
import json
import urllib
import requests
from datetime import datetime


def getStepProtocol(lims, step_id=None, step_uri=None):
	protocol_name = None
	if step_uri:
		step_config = Step(lims, uri=step_uri).configuration
		protocol_uri = re.sub("\/steps\/\d+","",step_config.uri)
		protocol_name = ProtocolStep(lims, uri=protocol_uri).name
	else:
		step_config = Step(lims, id=step_id).configuration
		protocol_uri = re.sub("\/steps\/\d+","",step_config.uri)
		protocol_name = ProtocolStep(lims, uri=protocol_uri).name
	return protocol_name



def getSeqFinance(lims, step_uri):
	"""Calculates the costs for all sequencing runs included in the step"""
	seq_finance = []

	step_details = Step(lims, uri=step_uri).details
	runs = {}
	pool_samples = {}

	pid_sequenced = {

	}
	for io_map in step_details.input_output_maps:
		if io_map[1]['output-generation-type'] == 'PerAllInputs': continue #Skip the billing table file
		pool = io_map[0]['uri']

		pool_samples[pool.id] = {}
		runs[pool.id] = {}
		run_date = None
		run_process_id = None
		pool_parent_process = pool.parent_process #BCL to FastQ


		#Try to determine the date this run was sequenced
		if pool_parent_process:
			for io_map in pool_parent_process.input_output_maps:
				if io_map[1]['limsid'] == pool.id:
					processes = lims.get_processes(inputartifactlimsid=io_map[0]['limsid'])
					if len(processes) == 1: #Only when run step has been skipped in LIMS (for whatever reason)
						run_date = io_map[0]['uri'].parent_process.date_run
						run_process_id = io_map[0]['uri'].parent_process.id

					for process in processes:
						if process.type.name in Config.RUN_PROCESSES:
							run_date = process.date_run
							run_process_id = process.id


		for sample in pool.samples:
			project_id = sample.project.id
			if project_id not in pool_samples[pool.id]:
				pool_samples[pool.id][project_id] = []
			pool_samples[pool.id][project_id].append(sample)


		for project_id in pool_samples[pool.id]:

			application = None
			platform = None
			run_meta = {
				'Application' : None,
				'Platform' : None,
				'Sequencing Runtype' : None,
				'Library prep kit' : None,
				'Number Lanes' : None,#default
				'date' : None,
				'samples' : []
			}
			if project_id not in pid_sequenced:
				pid_sequenced[project_id] = set()
			if project_id not in runs[pool.id]:
				runs[pool.id][project_id] = {
					'errors' : set(),
					'platform' : None,
					'name' : None,'id' : None,'open_date' : None,'nr_samples_submitted' : 0,'nr_samples_isolated' : 0,'nr_samples_prepped' : 0,'nr_samples_sequenced' : 0,'nr_samples_analyzed' : 0,'first_submission_date' : None,'received_date' : set(), 'project_comments' : None,'times_sequenced' : 0,#project fields
					'pool' : pool.id,
					'lims_runtype' : None,		'requested_runtype' : set(),		'run_personell_costs' : 0, 'run_step_costs':0,		'run_date' : None,'run_lanes' : None,	'succesful' : None, #run fields
					'lims_isolation' : set(),	'type' : set(),						'isolation_personell_costs' : 0,	'isolation_step_costs':0, 'isolation_date' : set(), #isolation fields
					'lims_library_prep' : set(),'requested_library_prep' : set(),	'libprep_personell_costs' : 0,	'libprep_step_costs':0,'libprep_date' : set(), #libprep fields
					'lims_analysis' : set(),	'requested_analysis' : set(),		'analysis_personell_costs' : 0,	'analysis_step_costs': 0,'analysis_date' : set(), #analysis fields
					'total_step_costs' :0,'total_personell_costs':0,
					'contact_name' : None,'contact_email' : None,'lab_name' : None,'budget_nr' : None,'order_nr' : None,'institute' : None,'postalcode' : None,'city' : None,'country' : None,'department' : None,'street' : None,
					'vat_nr' : None, 'deb_nr' : None
			}
			samples = lims.get_samples(projectlimsid=project_id)
			if len(pool.samples) != len(samples):
				runs[pool.id][project_id]['errors'].add('Warning : Number of samples sequenced not equal to number of samples submitted!')

			for sample in samples:
				sample_meta = {
					'Sample Type' : '',
					'Analysis' : '', #optional
					'Sequencing Coverage' : 0, #optional
					'Reads' : 0,
					'Add-ons' : [],
					'Isolated' : False,
					'Isolated_date' : None,
					'Prepped' : False,
					'Prepped_date' : None,
					'Sequenced' : False,
					'Analyzed' : False,
					'Analyzed_date' : None
				}
				#Added fields nr_samples_submitted, nr_samples_isolated, nr_samples_prepped, nr_samples_sequenced, nr_samples_analyzed
				runs[pool.id][project_id]['nr_samples_submitted'] += 1
				runs[pool.id][project_id]['received_date'].add(sample.date_received)

				runs[pool.id][project_id]['type'].add(sample.udf['Sample Type'])
				sample_meta['Sample Type'] = sample.udf['Sample Type']

				if 'Library prep kit' in sample.udf:
					runs[pool.id][project_id]['requested_library_prep'].add(sample.udf['Library prep kit'])
					run_meta['Library prep kit'] = sample.udf['Library prep kit']

				if 'Sequencing Coverage' in sample.udf:
					sample_meta['Sequencing Coverage'] = sample.udf['Sequencing Coverage']

				runs[pool.id][project_id]['requested_runtype'].add(sample.udf['Sequencing Runtype'])
				run_meta['Sequencing Runtype'] = sample.udf['Sequencing Runtype']

				if sample.udf['Platform'] == 'NovaSeq X':
					if 'Number Lanes' in sample.udf:
						runs[pool.id][project_id]['run_lanes'] = sample.udf['Number Lanes']
						run_meta['Number Lanes'] = sample.udf['Number Lanes']
					else:
						runs[pool.id][project_id]['run_lanes'] = 8
						run_meta['Number Lanes'] = 8
						runs[pool.id][project_id]['errors'].add('Warning : No Number Lanes UDF found, using default of 8 (all) lanes!')
				elif 'GEM-X' in sample.udf['Sequencing Runtype']:
					nr_cells = int(sample.udf.get('Number Cells'))
					reads_cell = int(sample.udf.get('Reads Per Cell'))
					reads = round((nr_cells * reads_cell)/1e6)
					sample_meta['Reads'] = reads

					sample_meta['Add-ons'] = sample.udf.get('Add-ons', [])

				if not platform:
					platform = sample.udf['Platform']

				if not application:
					application = sample.project.udf['Application']

				if application == 'USF - Sequencing': #old application name support
					if platform == 'Oxford Nanopore':
						application = 'ONT Sequencing'
					elif 'Illumina' in platform:
						application = 'Illumina Sequencing'
					elif 'Chromium' in platform:

						application = '10X Single Cell'
						platform = 'Chromium X'

				elif application == 'USF - SNP genotyping' or application == 'SNP Fingerprinting': #old application name support
					application = 'SNP Fingerprinting'
					if platform not in ['SNP Fingerprinting','60 SNP NimaGen panel']:
						platform = '60 SNP NimaGen panel'


				runs[pool.id][project_id]['platform'] = platform.replace('Illumina ', '')
				run_meta['Platform'] = platform.replace('Illumina ', '')

				runs[pool.id][project_id]['application'] = application
				run_meta['Application'] = application


				if 'Flowcell only' in sample.udf['Sequencing Runtype']:
					sample_meta['Sequenced'] = True
					run_meta['samples'].append(sample_meta)
					run_meta['date'] = sample.date_received
					runs[pool.id][project_id]['run_date'] = run_meta['date']

				else:
					sample_artifacts = lims.get_artifacts(samplelimsid=sample.id)
					for sample_artifact in sample_artifacts:

						if not sample_artifact.parent_process or not sample_artifact.parent_process.date_run: continue

						process_name = sample_artifact.parent_process.type.name

						if process_name in Config.ISOLATION_PROCESSES :
							if sample_artifact.type == 'ResultFile':continue

							isolation_type = "{0} isolation".format(sample_artifact.udf['US Isolation Type'].split(" ")[0].lower())

							if not sample_meta['Isolated']:
								runs[pool.id][project_id]['nr_samples_isolated'] +=1
								sample_meta['Isolated_date'] = sample_artifact.parent_process.date_run
								sample_meta['Isolated'] = True
							runs[pool.id][project_id]['lims_isolation'].add(sample_artifact.udf['US Isolation Type'])
							runs[pool.id][project_id]['isolation_date'].add(sample_artifact.parent_process.date_run)

							if isolation_type == 'rna isolation' and sample.udf['Sample Type'] != 'RNA unisolated':
								runs[pool.id][project_id]['errors'].add("Warning : Isolation type {0} in LIMS doesn't match sample type {1}".format(isolation_type, sample.udf['Sample Type']))
							elif isolation_type == 'dna isolation' and sample.udf['Sample Type'] != 'DNA unisolated':
								runs[pool.id][project_id]['errors'].add("Warning : Isolation type {0} in LIMS doesn't match sample type {1}".format(isolation_type, sample.udf['Sample Type']))

						elif process_name in Config.LIBPREP_PROCESSES and sample.udf['Sequencing Runtype'] not in ['WGS at HMF', 'WGS','WES (100X Coverage)','RNA-seq'] :

							if sample_artifact.type == 'ResultFile':continue

							lims_library_prep = ''

							if 'flongle' in sample.udf['Sequencing Runtype'].lower():
								lims_library_prep = 'nanopore flongle library prep'
							elif 'minion' in sample.udf['Sequencing Runtype'].lower() or 'promethion' in sample.udf['Sequencing Runtype'].lower():
								lims_library_prep = 'nanopore minion library prep'
							elif 'gem-x' in sample.udf['Sequencing Runtype'].lower():
								# lims_library_prep = sample.udf['Library prep kit'].lower()
								lims_library_prep = sample.udf['Sequencing Runtype'].lower()
							else:
								protocol_name = getStepProtocol(lims, step_id=sample_artifact.parent_process.id)
								lims_library_prep = protocol_name.split("-",1)[1].lower().strip()
								lims_library_prep = lims_library_prep.replace('illumina ', '')

							if not sample_meta['Prepped']:
								runs[pool.id][project_id]['nr_samples_prepped'] +=1
								sample_meta['Prepped_date'] = sample_artifact.parent_process.date_run
								sample_meta['Prepped'] = True
							runs[pool.id][project_id]['lims_library_prep'].add(lims_library_prep)
							runs[pool.id][project_id]['libprep_date'].add(sample_artifact.parent_process.date_run)

						elif process_name in Config.RUN_PROCESSES or process_name in Config.LOAD_PROCESSES:

							protocol_name = getStepProtocol(lims, step_id=sample_artifact.parent_process.id)
							runs[pool.id][project_id]['lims_runtype'] = protocol_name.split("-",1)[1].lower().strip()

							if not runs[pool.id][project_id]['run_date']:
								runs[pool.id][project_id]['run_date'] = run_date
								runs[pool.id][project_id]['times_sequenced'] = 1
								run_meta['date'] = runs[pool.id][project_id]['run_date']
							pid_sequenced[project_id].add(sample_artifact.parent_process.date_run)

							if not sample_meta['Sequenced']:
								runs[pool.id][project_id]['nr_samples_sequenced'] +=1
								sample_meta['Sequenced'] = True

						elif process_name in Config.ANALYSIS_PROCESSES:
							runs[pool.id][project_id]['analysis_date'].add(sample_artifact.parent_process.date_run)
							analysis_steps =['Raw data (FastQ)']
							if sample_artifact.parent_process.udf['Mapping']:
								analysis_steps.append('Mapping')
							if sample_artifact.parent_process.udf['Germline SNV/InDel calling']:
								analysis_steps.append('Germline SNV/InDel calling')
							if sample_artifact.parent_process.udf['Read count analysis (mRNA)']:
								analysis_steps.append('Read count analysis (mRNA)')
							if sample_artifact.parent_process.udf['Differential expression analysis + figures (mRNA)']:
								analysis_steps.append('Differential expression analysis + figures (mRNA)')
							if sample_artifact.parent_process.udf['CNV + SV calling']:
								analysis_steps.append('CNV + SV calling')
							if sample_artifact.parent_process.udf['Somatic calling (tumor/normal pair)']:
								analysis_steps.append('Somatic calling (tumor/normal pair)')
							runs[pool.id][project_id]['requested_analysis'].add("|".join(sorted(sample.udf['Analysis'].split(","))))
							sample_meta['Analysis'] = sample.udf['Analysis']
							runs[pool.id][project_id]['lims_analysis'].add("|".join( sorted( analysis_steps) ))

							if runs[pool.id][project_id]['requested_analysis'] != runs[pool.id][project_id]['lims_analysis']:
								runs[pool.id][project_id]['errors'].add("Warning : Analysis type {0} in LIMS doesn't match analysis {1}".format(runs[pool.id][project_id]['lims_analysis'], runs[pool.id][project_id]['requested_analysis']))

							if not sample_meta['Analyzed']:
								runs[pool.id][project_id]['nr_samples_analyzed'] +=1
								sample_meta['Analyzed_date'] = sample_artifact.parent_process.date_run
								sample_meta['Analyzed'] = True

		#
				run_meta['samples'].append(sample_meta)

				if not runs[pool.id][project_id]['name'] :
					runs[pool.id][project_id]['first_submission_date'] = sample.date_received

					if 'Sequencing Succesful' in pool.udf :
						runs[pool.id][project_id]['succesful'] = pool.udf['Sequencing Succesful']

					runs[pool.id][project_id]['name'] = sample.project.name
					runs[pool.id][project_id]['id'] = sample.project.id
					runs[pool.id][project_id]['open_date'] = sample.project.open_date

					if 'Comments and agreements' in sample.project.udf:
						runs[pool.id][project_id]['project_comments'] = sample.project.udf['Comments and agreements']
						runs[pool.id][project_id]['project_comments'] = runs[pool.id][project_id]['project_comments'].replace('\n', ' ').replace('\r', '').replace(';', ',')

					runs[pool.id][project_id]['contact_name'] = sample.project.researcher.first_name + " " + sample.project.researcher.last_name
					runs[pool.id][project_id]['contact_email'] = sample.project.researcher.email
					runs[pool.id][project_id]['lab_name'] = sample.project.researcher.lab.name

					if 'Budget Number' in sample.udf:
						runs[pool.id][project_id]['budget_nr'] = sample.udf['Budget Number']
					else:
						runs[pool.id][project_id]['errors'].add("Warning : No Budgetnumber found")

					if 'Order Number' in sample.udf:
						runs[pool.id][project_id]['order_nr'] = sample.udf['Order Number']

					runs[pool.id][project_id]['institute'] = sample.project.researcher.lab.billing_address['institution']
					runs[pool.id][project_id]['postalcode'] = sample.project.researcher.lab.billing_address['postalCode']
					runs[pool.id][project_id]['city'] = sample.project.researcher.lab.billing_address['city']
					runs[pool.id][project_id]['country'] = sample.project.researcher.lab.billing_address['country']
					runs[pool.id][project_id]['department'] = sample.project.researcher.lab.billing_address['department']
					runs[pool.id][project_id]['street'] = sample.project.researcher.lab.billing_address['street']
					if 'UMCU_DebNr' in sample.project.researcher.lab.udf: runs[pool.id][project_id]['deb_nr'] = sample.project.researcher.lab.udf['UMCU_DebNr']
					if 'UMCU_VATNr' in sample.project.researcher.lab.udf: runs[pool.id][project_id]['vat_nr'] = sample.project.researcher.lab.udf['UMCU_VATNr']


			#calculate costs here
			url = f'{Config.PORTAL_URL}/finance/projectcosts/{project_id}'
			headers = {
				'Content-type' : 'application/json; charset=utf-8',
				'Authorization' : f'Bearer {Config.PORTAL_API_KEY}'
			}

			if len(pid_sequenced[project_id]) > 1:
				runs[pool.id][project_id]['errors'].add("Warning : Run was sequenced before more than once!")
				#In case of reruns always bill at the costs of the oldest run
				run_meta['date'] = min( pid_sequenced[project_id] )

			if not run_meta['date']:
				run_meta['date'] = min( pid_sequenced[project_id] )
				sample_meta['Sequenced'] = True
				runs[pool_id][project_id]['run_date'] = run_meta['date']


			data = json.dumps(run_meta, indent=4)

			response = requests.post(url, headers=headers, data=data)
			costs = response.json()

			if 'error' in costs:
				runs[pool.id][project_id]['errors'].add(costs['error'])
			else:
				runs[pool.id][project_id]['isolation_step_costs'] = "{:.2f}".format(float( costs['Isolation']['step_cost'] ))
				runs[pool.id][project_id]['isolation_personell_costs'] = "{:.2f}".format(float( costs['Isolation']['personell_cost'] ))
				runs[pool.id][project_id]['libprep_step_costs'] = "{:.2f}".format(float( costs['Library Prep']['step_cost'] ))
				runs[pool.id][project_id]['libprep_personell_costs'] = "{:.2f}".format(float( costs['Library Prep']['personell_cost'] ))
				runs[pool.id][project_id]['run_step_costs'] = "{:.2f}".format(float( costs[application]['step_cost'] ))
				runs[pool.id][project_id]['run_personell_costs'] = "{:.2f}".format(float( costs[application]['personell_cost'] ))
				runs[pool.id][project_id]['analysis_step_costs'] = "{:.2f}".format(float( costs['Analysis']['step_cost'] ))
				runs[pool.id][project_id]['analysis_personell_costs'] = "{:.2f}".format(float( costs['Analysis']['personell_cost'] ))
				runs[pool.id][project_id]['total_step_costs'] = "{:.2f}".format(float( costs['Total']['step_cost'] ))
				runs[pool.id][project_id]['total_personell_costs'] = "{:.2f}".format(float( costs['Total']['personell_cost'] ))

	runs_dedup = {}
	unique_ids = []
	for pool_id in runs:
		for project_id in runs[pool_id]:

			unique_id = f"{project_id}-{runs[pool_id][project_id]['run_date']}"
			if unique_id not in unique_ids:
				unique_ids.append(unique_id)
				runs_dedup[pool_id] = {}
				runs_dedup[pool_id][project_id] =  runs[pool_id][project_id]
				for k,v in runs_dedup[pool_id][project_id].items():
					if isinstance(v,set):
						runs_dedup[pool_id][project_id][k] = ",".join(list(v))
	return renderTemplate('seq_finance_overview_template.csv', {'pools':runs_dedup})

def getSnpFinance(lims, step_uri):

    seq_finance = []

    step_details = Step(lims, uri=step_uri).details

    runs = {}
    #Get the input artifacts (which is a pool of samples)
    for io_map in step_details.input_output_maps:
        pool = io_map[0]['uri']
    #
        for sample in pool.samples:
            try :
                budget_nr = sample.udf['Budget Number']
            except:
                sys.exit(f'No budgetnumber found for run {sample.project.id}')

            if sample.project.id + budget_nr not in runs:
                runs[ sample.project.id + budget_nr ] ={
                    'errors' : set(),
                    'name' : sample.project.name,
                    'id' : sample.project.id,
                    'open_date' : sample.project.open_date,
                    'samples' : {},
                    'first_submission_date' : None,
                    'received_date' : None, #project fields
                    'description' : set(),
                    'type' : set(),
                    'isolation_step_costs' : 0,
                    'isolation_personell_costs' : 0,
                    'plate_step_costs' : 0,
                    'plate_personell_costs' : 0,
                    'total_step_costs' :0,
                    'total_personell_costs' :0,
                    'contact_name' : sample.project.researcher.first_name + " " + sample.project.researcher.last_name,
                    'contact_email' : sample.project.researcher.email,
                    'lab_name' : sample.project.researcher.lab.name,
                    'budget_nr' : budget_nr,
                    'institute' : sample.project.researcher.lab.billing_address['institution'],
                    'postalcode' : sample.project.researcher.lab.billing_address['postalCode'],
                    'city' : sample.project.researcher.lab.billing_address['city'],
                    'country' : sample.project.researcher.lab.billing_address['country'],
                    'department' : sample.project.researcher.lab.billing_address['department'],
                    'street' : sample.project.researcher.lab.billing_address['street']
                }
            if pool.id + sample.id not in runs[ sample.project.id + budget_nr ]['samples']:
                runs[ sample.project.id + budget_nr ]['samples'][pool.id + sample.id] = sample

                runs[sample.project.id + budget_nr]['received_date'] = sample.date_received
                runs[sample.project.id + budget_nr]['type'].add(sample.udf['Sample Type'])
                if 'Description' in sample.udf:
                    runs[sample.project.id + budget_nr]['description'].add(sample.udf['Description'])

    for id in runs:
        run_meta = {
            'Application' : 'SNP Fingerprinting',
            'Platform' : '60 SNP NimaGen panel',
            'Sequencing Runtype' : '60 SNP NimaGen panel',
            'Library prep kit' : None,
            'Number Lanes' : None,#default
            'date' : runs[id]['received_date'],
            'samples' : []
        }
        for sample_id in runs[id]['samples']:
            sample = runs[id]['samples'][sample_id]
            run_meta['samples'].append(
                {
                    'Sample Type' : sample.udf['Sample Type'],
                    'Analysis' : '', #optional
                    'Sequencing Coverage' : 0, #optional
                    'Isolated' : True if 'unisolated' in sample.udf['Sample Type'] else False,
                    'Prepped' : False,
                    'Sequenced' : False,
                    'Analyzed' : False,
                }
            )
        #calculate costs here
        url = f'{Config.PORTAL_URL}/finance/projectcosts/{runs[id]["id"]}'
        headers = {
          'Content-type' : 'application/json; charset=utf-8',
          'Authorization' : f'Bearer {Config.PORTAL_API_KEY}'
        }

        data = json.dumps(run_meta)
        response = requests.post(url, headers=headers, data=data)
        costs = response.json()
        runs[id]['isolation_step_costs'] = "{:.2f}".format(float( costs['Isolation']['step_cost'] ))
        runs[id]['isolation_personell_costs'] = "{:.2f}".format(float( costs['Isolation']['personell_cost'] ))
        runs[id]['plate_step_costs'] = "{:.2f}".format(float( costs['SNP Fingerprinting']['step_cost'] ))
        runs[id]['plate_personell_costs'] = "{:.2f}".format(float( costs['SNP Fingerprinting']['personell_cost'] ))
        runs[id]['total_step_costs'] = "{:.2f}".format(float( costs['Total']['step_cost'] ))
        runs[id]['total_personell_costs'] = "{:.2f}".format(float( costs['Total']['personell_cost'] ))

    #
    return renderTemplate('snp_finance_overview_template.csv', {'runs':runs})

def run(lims, step_uri, output_file):
	"""Runs the getSeqFinance or the getSnpFinance depending on the protocol the step is in"""

	protocol_name = getStepProtocol(lims,step_uri=step_uri)

	if protocol_name.startswith("USEQ - Post Sequencing"):
		finance_table = getSeqFinance(lims,step_uri)
		output_file.write(finance_table)
	elif protocol_name.startswith("USEQ - Post Fingerprinting"):
		finance_table = getSnpFinance(lims, step_uri)
		output_file.write(finance_table)
