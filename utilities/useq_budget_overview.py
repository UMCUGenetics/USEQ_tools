from genologics.entities import Step, ProtocolStep
from modules.useq_template import TEMPLATE_PATH,TEMPLATE_ENVIRONMENT,renderTemplate
from config import COST_DB,RUN_PROCESSES,ISOLATION_PROCESSES,LIBPREP_PROCESSES,ANALYSIS_PROCESSES
import re
import sys
import json
import urllib

def getAllCosts():
	"""Retrieves costs from cost db"""
	costs_json = ""
	try:
		costs_json = urllib.urlopen( COST_DB ).read()
	except urllib.error.HTTPError as e:
		sys.exit(e.msg)
	except urllib.error.URLError as e:
		sys.exit(e.read())
	except:
		sys.exit( str(sys.exc_type) + str(sys.exc_value) )

	costs = json.loads(costs_json)

	costs_lower = dict( (k.lower(), v) for k,v in costs.iteritems())
	#Do some (unfortunately) neccessary name conversions
	costs_lower['libprep dna'] = costs_lower['truseq dna nano']
	costs_lower['truseq dna nano (manual)'] = costs_lower['truseq dna nano']
	costs_lower['truseq dna nano (automatic)'] = costs_lower['truseq dna nano']
	costs_lower['truseq rna stranded polya (manual)'] =costs_lower['truseq rna stranded polya']
	costs_lower['truseq rna stranded polya (automatic)'] =costs_lower['truseq rna stranded polya']
	costs_lower['libprep rna stranded polya'] = costs_lower['truseq rna stranded polya']
	costs_lower['truseq rna stranded ribo-zero'] = costs_lower['truseq rna stranded ribozero (human, mouse, rat)']
	costs_lower['libprep rna stranded ribo-zero'] = costs_lower['truseq rna stranded ribozero (human, mouse, rat)']
	costs_lower['open snp array'] = costs_lower['snp open array (60 snps)']

	return costs_lower

def getNearestBillingDate(all_costs, step ,step_date):
	billing_date = ''

	for date in sorted(all_costs[ step ][ 'date_step_costs'].keys() ):

		if date <= step_date:
			billing_date = date

	if not billing_date:
		billing_date = sorted(all_costs[ step ][ 'date_step_costs'].keys())[-1]

	return billing_date

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


def getOverview(lims,bnr):

	# samples = lims.get_samples(udf={'Budget Number':bnr})
	samples = lims.get_samples()
	ovw_snp = {}
	ovw_seq = {}
	snp_samples = []
	seq_samples = []
	all_costs = getAllCosts()

	for sample in samples:
		if 'Budget Number' in sample.udf and bnr in sample.udf['Budget Number']:
			project = sample.project
			if project.udf['Application'] == 'USF - SNP genotyping' or project.udf['Application'] == 'Research':
				snp_samples.append(sample)
			else:
				seq_samples.append(sample)

	for sample in seq_samples:
		project = sample.project
		if project.id not in ovw_seq:
			ovw_seq[project.id] = {
				'errors' : set(),
				'name' : None,'id' : None,'open_date' : None,'nr_samples' : 0,'first_submission_date' : None,'received_date' : set(), #project fields
				'lims_runtype' : None,		'requested_runtype' : set(),		'run_personell_costs' : 0, 'run_step_costs':0,		'run_date' : None,			'succesful' : None, #run fields
				'lims_isolation' : set(),	'type' : set(),						'isolation_personell_costs' : 0,	'isolation_step_costs':0, 'isolation_date' : set(), #isolation fields
				'lims_library_prep' : set(),'requested_library_prep' : set(),	'libprep_personell_costs' : 0,	'libprep_step_costs':0,'libprep_date' : set(), #libprep fields
				'lims_analysis' : set(),	'requested_analysis' : set(),		'analysis_personell_costs' : 0,	'analysis_step_costs': 0,'analysis_date' : set(), #analysis fields
				'total_step_costs' :0,'total_personell_costs':0,
				'contact_name' : None,'contact_email' : None,'lab_name' : None,'budget_nr' : None,'institute' : None,'postalcode' : None,'city' : None,'country' : None,'department' : None,'street' : None,
			}
		ovw_seq[project.id]['nr_samples'] +=1
		ovw_seq[project.id]['received_date'].add(sample.date_received)
		ovw_seq[project.id]['type'].add(sample.udf['Sample Type'])
		ovw_seq[project.id]['requested_library_prep'].add(sample.udf['Library prep kit'])
		ovw_seq[project.id]['requested_runtype'].add(sample.udf['Sequencing Runtype'])

		sample_artifacts = lims.get_artifacts(samplelimsid=sample.id)
		for sample_artifact in sample_artifacts:

			if not sample_artifact.parent_process: continue
			process_name = sample_artifact.parent_process.type.name

			if process_name in ISOLATION_PROCESSES:

				if 'US Isolation Type' in sample_artifact.udf:
					isolation_type = "{0} isolation".format(sample_artifact.udf['US Isolation Type'].split(" ")[0].lower())

					billing_date = getNearestBillingDate(all_costs, isolation_type , sample_artifact.parent_process.date_run)

					ovw_seq[project.id]['isolation_step_costs'] += float(all_costs[ isolation_type ][ 'date_step_costs' ][ billing_date ])
					ovw_seq[project.id]['isolation_personell_costs'] += float(all_costs[ isolation_type ][ 'date_personell_costs' ][ billing_date ])
					ovw_seq[project.id]['total_step_costs']+= float(all_costs[ isolation_type ][ 'date_step_costs' ][ billing_date ])
					ovw_seq[project.id]['total_personell_costs']+= float(all_costs[ isolation_type ][ 'date_personell_costs' ][ billing_date ])
					ovw_seq[project.id]['lims_isolation'].add(sample_artifact.udf['US Isolation Type'])
					ovw_seq[project.id]['isolation_date'].add(sample_artifact.parent_process.date_run)

					if isolation_type == 'rna isolation' and sample.udf['Sample Type'] != 'RNA unisolated':
						ovw_seq[project.id]['errors'].add("Isolation type {0} in LIMS doesn't match sample type {1}".format(isolation_type, sample.udf['Sample Type']))
					elif isolation_type == 'dna isolation' and sample.udf['Sample Type'] != 'DNA unisolated':
						ovw_seq[project.id]['errors'].add("Isolation type {0} in LIMS doesn't match sample type {1}".format(isolation_type, sample.udf['Sample Type']))
					else:
						ovw_seq[project.id]['errors'].add("Could not find isolation type")
			elif process_name in LIBPREP_PROCESSES:
				protocol_name = getStepProtocol(lims, step_id=sample_artifact.parent_process.id)

				if '-' in protocol_name[0:10]:
					lims_library_prep = protocol_name.split("-",1)[1].lower().strip()
				else:
					lims_library_prep = protocol_name.split(":",1)[1].lower().strip()

				ovw_seq[project.id]['lims_library_prep'].add(lims_library_prep)

				billing_date = getNearestBillingDate(all_costs, lims_library_prep , sample_artifact.parent_process.date_run)
				ovw_seq[project.id]['libprep_step_costs'] += float(all_costs[ lims_library_prep][ 'date_step_costs' ][ billing_date ])
				ovw_seq[project.id]['libprep_personell_costs'] += float(all_costs[ lims_library_prep][ 'date_personell_costs' ][ billing_date ])
				ovw_seq[project.id]['total_step_costs'] += float(all_costs[ lims_library_prep][ 'date_step_costs' ][ billing_date ])
				ovw_seq[project.id]['total_personell_costs'] += float(all_costs[ lims_library_prep][ 'date_personell_costs' ][ billing_date ])
				ovw_seq[project.id]['libprep_date'].add(sample_artifact.parent_process.date_run)

				if sample.udf['Library prep kit'] == 'Truseq RNA stranded polyA' and 'libprep rna stranded polya' not in ovw_seq[project.id]['lims_library_prep']:
					ovw_seq[project.id]['errors'].add("Libprep type {0} in LIMS doesn't match libprep type {1}".format(ovw_seq[project.id]['lims_library_prep'],sample.udf['Library prep kit']))

				elif sample.udf['Library prep kit'] == 'Truseq RNA stranded ribo-zero' and 'libprep rna stranded ribo-zero' not in ovw_seq[project.id]['lims_library_prep'] :
					ovw_seq[project.id]['errors'].add("Libprep type {0} in LIMS doesn't match libprep type {1}".format(ovw_seq[project.id]['lims_library_prep'],sample.udf['Library prep kit']))

				elif sample.udf['Library prep kit'] == 'Truseq DNA nano' and 'libprep dna' not in ovw_seq[project.id]['lims_library_prep'] :
					ovw_seq[project.id]['errors'].add("Libprep type {0} in LIMS doesn't match libprep type {1}".format(ovw_seq[project.id]['lims_library_prep'],sample.udf['Library prep kit']))

			elif process_name in RUN_PROCESSES and not ovw_seq[project.id]['lims_runtype']:
				protocol_name = getStepProtocol(lims, step_id=sample_artifact.parent_process.id)

				if '-' in protocol_name:
					ovw_seq[project.id]['lims_runtype'] = protocol_name.split("-",1)[1].lower().strip()
				else:
					ovw_seq[project.id]['lims_runtype'] = protocol_name.split(":",1)[1].lower().strip()
				requested_runtype = sample.udf['Sequencing Runtype'].lower()
				billing_date = getNearestBillingDate(all_costs, requested_runtype , sample_artifact.parent_process.date_run)
				ovw_seq[project.id]['run_step_costs'] = float(all_costs[ requested_runtype ][ 'date_step_costs' ][ billing_date ])
				ovw_seq[project.id]['run_personell_costs'] = float(all_costs[ requested_runtype ][ 'date_personell_costs' ][ billing_date ])
				ovw_seq[project.id]['total_step_costs'] += float(all_costs[ requested_runtype ][ 'date_step_costs' ][ billing_date ])
				ovw_seq[project.id]['total_personell_costs'] += float(all_costs[ requested_runtype ][ 'date_personell_costs' ][ billing_date ])
				ovw_seq[project.id]['run_date'] = sample_artifact.parent_process.date_run

				# if ovw_seq[project.id]['lims_runtype'].split(" ")[0] not in ",".join(ovw_seq[project.id]['requested_runtype']).lower():
				# ovw_seq[project.id]['errors'].add("Run type {0} in LIMS doesn't match run type {1}".format(ovw_seq[project.id]['lims_runtype'],",".join(ovw_seq[project.id]['requested_runtype'])))
			elif process_name in ANALYSIS_PROCESSES:
				billing_date = getNearestBillingDate(all_costs, 'mapping wgs' , sample_artifact.parent_process.date_run)
				ovw_seq[project.id]['analysis_date'].add(sample_artifact.parent_process.date_run)
				analysis_steps =['Raw data (FastQ)']
				analysis_step_costs = 0
				analysis_personell_costs = 0
				if sample_artifact.parent_process.udf['Mapping']:
					analysis_steps.append('Mapping')
					if sample.udf['Sample Type'].startswith('RNA'):
						analysis_step_costs += float(all_costs['mapping rna']['date_step_costs'][ billing_date ])
						analysis_personell_costs += float(all_costs['mapping rna']['date_personell_costs'][ billing_date ])
					else:
						analysis_step_costs += float(all_costs['mapping wgs']['date_step_costs'][ billing_date ])
						analysis_personell_costs += float(all_costs['mapping wgs']['date_personell_costs'][ billing_date ])
				if sample_artifact.parent_process.udf['Germline SNV/InDel calling']:
					analysis_steps.append('Germline SNV/InDel calling')
					analysis_step_costs += float(all_costs['germline snv/indel calling']['date_step_costs'][ billing_date ])
					analysis_personell_costs += float(all_costs['germline snv/indel calling']['date_personell_costs'][ billing_date ])
				if sample_artifact.parent_process.udf['Read count analysis (mRNA)']:
					analysis_steps.append('Read count analysis (mRNA)')
					analysis_step_costs += float(all_costs['read count analysis (mrna)']['date_step_costs'][ billing_date ])
					analysis_personell_costs += float(all_costs['read count analysis (mrna)']['date_personell_costs'][ billing_date ])
				if sample_artifact.parent_process.udf['Differential expression analysis + figures (mRNA)']:
					analysis_steps.append('Differential expression analysis + figures (mRNA)')
					analysis_step_costs += float(all_costs['differential expression analysis + figures (mrna)']['date_step_costs'][ billing_date ])
					analysis_personell_costs += float(all_costs['differential expression analysis + figures (mrna)']['date_personell_costs'][ billing_date ])
				if sample_artifact.parent_process.udf['CNV + SV calling']:
					analysis_steps.append('CNV + SV calling')
					analysis_step_costs += float(all_costs['cnv + sv calling']['date_step_costs'][ billing_date ])
					analysis_personell_costs += float(all_costs['cnv + sv calling']['date_personell_costs'][ billing_date ])
				if sample_artifact.parent_process.udf['Somatic calling (tumor/normal pair)']:
					analysis_steps.append('Somatic calling (tumor/normal pair)')
					analysis_step_costs += float(all_costs['somatic calling (tumor/normal pair)']['date_step_costs'][ billing_date ])
					analysis_personell_costs += float(all_costs['somatic calling (tumor/normal pair)']['date_personell_costs'][ billing_date ])

				ovw_seq[project.id]['requested_analysis'].add("|".join(sorted(sample.udf['Analysis'].split(","))))
				ovw_seq[project.id]['lims_analysis'].add("|".join( sorted( analysis_steps) ))
				ovw_seq[project.id]['analysis_step_costs'] += analysis_step_costs
				ovw_seq[project.id]['analysis_personell_costs'] += analysis_personell_costs
				ovw_seq[project.id]['total_step_costs'] += analysis_step_costs
				ovw_seq[project.id]['total_personell_costs'] += analysis_personell_costs
				if ovw_seq[project.id]['requested_analysis'] != ovw_seq[project.id]['lims_analysis']:
					ovw_seq[project.id]['errors'].add("Analysis type {0} in LIMS doesn't match analysis {1}".format(ovw_seq[project.id]['lims_analysis'], ovw_seq[project.id]['requested_analysis']))

		#Get billing specific info
		if not ovw_seq[project.id]['name'] :
			ovw_seq[project.id]['first_submission_date'] = sample.date_received

			# if 'Sequencing Succesful' in pool.udf : ovw_seq[project.id]['succesful'] = pool.udf['Sequencing Succesful']
			ovw_seq[project.id]['name'] = sample.project.name
			ovw_seq[project.id]['id'] = sample.project.id
			ovw_seq[project.id]['open_date'] = sample.project.open_date
			ovw_seq[project.id]['contact_name'] = sample.project.researcher.first_name + " " + sample.project.researcher.last_name
			ovw_seq[project.id]['contact_email'] = sample.project.researcher.email
			ovw_seq[project.id]['lab_name'] = sample.project.researcher.lab.name
			ovw_seq[project.id]['budget_nr'] = sample.udf['Budget Number']
			ovw_seq[project.id]['institute'] = sample.project.researcher.lab.billing_address['institution']
			ovw_seq[project.id]['postalcode'] = sample.project.researcher.lab.billing_address['postalCode']
			ovw_seq[project.id]['city'] = sample.project.researcher.lab.billing_address['city']
			ovw_seq[project.id]['country'] = sample.project.researcher.lab.billing_address['country']
			ovw_seq[project.id]['department'] = sample.project.researcher.lab.billing_address['department']
			ovw_seq[project.id]['street'] = sample.project.researcher.lab.billing_address['street']

	for sample in snp_samples:
		budget_nr = sample.udf['Budget Number']
		if sample.project.id + budget_nr not in ovw_snp:
			ovw_snp[ sample.project.id + budget_nr ] ={
				'errors' : set(),
				'name' : sample.project.name,
				'id' : sample.project.id,
				'open_date' : sample.project.open_date,
				'nr_samples' : 0,
				'first_submission_date' : None,
				'received_date' : set(), #project fields
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
		ovw_snp[sample.project.id + budget_nr]['nr_samples'] += 1
		ovw_snp[sample.project.id + budget_nr]['received_date'].add(sample.date_received)
		ovw_snp[sample.project.id + budget_nr]['type'].add(sample.udf['Sample Type'])
		if 'Description' in sample.udf:
			ovw_snp[sample.project.id + budget_nr]['description'].add(sample.udf['Description'])
		billing_date = getNearestBillingDate(all_costs, 'open snp array' , sample.date_received)
		# plate_costs = float(all_costs['open snp array']['date_costs'][ billing_date ]) / 4
		ovw_snp[sample.project.id + budget_nr]['plate_step_costs'] = float(all_costs['open snp array']['date_step_costs'][ billing_date ])
		ovw_snp[sample.project.id + budget_nr]['plate_personell_costs'] = float(all_costs['open snp array']['date_personell_costs'][ billing_date ])

		if not ovw_snp[sample.project.id + budget_nr]['total_step_costs']:
			ovw_snp[sample.project.id + budget_nr]['total_step_costs'] += ovw_snp[sample.project.id + budget_nr]['plate_step_costs']
			ovw_snp[sample.project.id + budget_nr]['total_personell_costs'] += ovw_snp[sample.project.id + budget_nr]['plate_personell_costs']

		if sample.udf['Sample Type'] == 'DNA unisolated':
			ovw_snp[sample.project.id + budget_nr]['isolation_step_costs'] += float( all_costs['dna isolation'][ 'date_step_costs' ][ billing_date ] )
			ovw_snp[sample.project.id + budget_nr]['total_step_costs'] += float( all_costs['dna isolation'][ 'date_step_costs' ][ billing_date ] )
			ovw_snp[sample.project.id + budget_nr]['isolation_personell_costs'] += float( all_costs['dna isolation'][ 'date_personell_costs' ][ billing_date ] )
			ovw_snp[sample.project.id + budget_nr]['total_personell_costs'] += float( all_costs['dna isolation'][ 'date_personell_costs' ][ billing_date ] )
		elif sample.udf['Sample Type'] == 'RNA unisolated':
			ovw_snp[sample.project.id + budget_nr]['isolation_step_costs'] += float( all_costs['rna isolation'][ 'date_step_costs' ][ billing_date ] )
			ovw_snp[sample.project.id + budget_nr]['total_step_costs'] += float( all_costs['rna isolation'][ 'date_step_costs' ][ billing_date ] )
			ovw_snp[sample.project.id + budget_nr]['isolation_personell_costs'] += float( all_costs['rna isolation'][ 'date_personell_costs' ][ billing_date ] )
			ovw_snp[sample.project.id + budget_nr]['total_personell_costs'] += float( all_costs['rna isolation'][ 'date_personell_costs' ][ billing_date ] )

	for id in ovw_snp:
		plate_step_costs = ovw_snp[id]['plate_step_costs']
		plate_personell_costs = ovw_snp[id]['plate_personell_costs']
		nr_samples = ovw_snp[id]['nr_samples']
		ovw_snp[id]['plate_step_costs'] = (plate_step_costs / 45) * nr_samples
		ovw_snp[id]['plate_personell_costs'] = (plate_personell_costs / 45) * nr_samples


	print ("Sequencing for {0}:".format(bnr))
	print (renderTemplate('seq_finance_overview_template.csv', {'runs':ovw_seq}))

	print ("Fingerprinting for {0}:".format(bnr))
	print (renderTemplate('snp_finance_overview_template.csv', {'runs':ovw_snp}))


def run(lims, budget_numbers, output_file):


    for bnr in budget_numbers.split(','):

        getOverview(lims, bnr)
