from genologics.entities import Step, ProtocolStep
from modules.useq_template import TEMPLATE_PATH,TEMPLATE_ENVIRONMENT,renderTemplate
from config import Config
import re
import sys
import json
import urllib
from datetime import datetime
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

def getFinanceOvw():
    Base = automap_base()
    ssl_args = {'ssl_ca': '/etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt'}
    engine = create_engine(Config.PORTAL_DB_URI, connect_args=ssl_args, pool_pre_ping=True)
    Base.prepare(engine, reflect=True)
    Step = Base.classes.step
    StepCost = Base.classes.step_cost
    session = Session(engine)

    step_costs = {}


    for step in session.query(Step).all():
        if step.name not in step_costs:
            step_costs[ step.name ] = {}

        for step_cost in session.query(StepCost).filter_by(step_id=step.id).all():
            step_costs[ step.name ][step_cost.date] = {
                'costs' : step_cost.step_cost + step_cost.personell_cost,
                'step_costs' : step_cost.step_cost,
                'personell_costs' : step_cost.personell_cost
            }

    return step_costs

def getAllCosts():
    """Retrieves costs from cost db"""

    costs = getFinanceOvw()
    costs_lower = dict( (k.lower(), v) for k,v in costs.items())
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
    costs_lower['mid output : 2 x 75 bp'] = costs_lower['nextseq500 2 x 75 bp mid output']
    costs_lower['mid output : 2 x 150 bp' ] = costs_lower[ 'nextseq500 2 x 150 bp mid output']
    costs_lower['high output : 1 x 75 bp' ] = costs_lower[ 'nextseq500 1 x 75 bp high output']
    costs_lower['high output : 2 x 75 bp' ] = costs_lower[ 'nextseq500 2 x 75 bp high output']
    costs_lower['high output : 2 x 150 bp' ] = costs_lower[ 'nextseq500 2 x 150 bp high output']
    costs_lower['v2 kit : 1 x 50 bp' ] = costs_lower[ 'miseq 1 x 50 bp v2 kit']
    costs_lower['v2 kit (nano) : 1 x 300 bp' ] = costs_lower[ 'miseq 1x300 bp v2 kit (nano)']
    costs_lower['v2 kit (micro) : 1 x 300 bp' ] = costs_lower[ 'miseq 1x300 bp v2 kit (micro)']
    costs_lower['v2 kit : 2 x 150 bp' ] = costs_lower[ 'miseq 2 x 150 bp v2 kit']
    costs_lower['v2 kit : 2 x 250 bp' ] = costs_lower[ 'miseq 2 x 250 bp v2 kit']
    costs_lower['v3 kit : 2 x 75 bp' ] = costs_lower[ 'miseq 2 x 75 bp v3 kit']
    costs_lower['v3 kit : 2 x 300 bp' ] = costs_lower[ 'miseq 2 x 300 bp v3 kit']
    costs_lower['s4 : 2 x 150 bp'] = costs_lower[ 'novaseq 6000 s4 2 x 150 bp']
    costs_lower['s1 : 2 x 50 bp' ] = costs_lower[ 'novaseq 6000 s1 2 x 50 bp']
    costs_lower['s1 : 2 x 100 bp'] = costs_lower['novaseq 6000 s1 2 x 100 bp' ]
    costs_lower['s1 : 2 x 150 bp'] = costs_lower['novaseq 6000 s1 2 x 150 bp' ]
    costs_lower['s2 : 2 x 50 bp' ] = costs_lower['novaseq 6000 s2 2 x 50 bp' ]
    costs_lower['s2 : 2 x 100 bp'] = costs_lower['novaseq 6000 s2 2 x 100 bp'  ]
    costs_lower['s2 : 2 x 150 bp'] = costs_lower['novaseq 6000 s2 2 x 150 bp'  ]
    costs_lower['s4 : 2 x 100 bp'] = costs_lower[ 'novaseq 6000 s4 2 x 100 bp' ]
    costs_lower['s4 : 1 x 35 bp'] = costs_lower['novaseq 6000 s4 1 x 35 bp']
    costs_lower['wgs at hmf'] = costs_lower['novaseq 6000 wgs at hmf']
    costs_lower['wgs'] = costs_lower['novaseq 6000 wgs at hmf']
    costs_lower['sp : 2 x 100 bp'] = costs_lower['novaseq 6000 sp 2 x 100 bp']
    costs_lower['sp : 2 x 50 bp'] = costs_lower[ 'novaseq 6000 sp 2 x 50 bp' ]
    costs_lower['sp : 2 x 150 bp'] = costs_lower['novaseq 6000 sp 2 x 150 bp' ]
    costs_lower['sp : 2 x 250 bp'] = costs_lower[ 'novaseq 6000 sp 2 x 250 bp']
    costs_lower['1 x minion flowcell' ] = costs_lower[ 'nanopore minion 1 x flowcell']
    costs_lower['1 x promethion flowcell' ] = costs_lower[ 'nanopore promethion 1 x flowcell']
    costs_lower['1 x flongle flowcell'] = costs_lower[ 'nanopore flongle 1 x flowcell']
    costs_lower['1 x minion flowcell (flowcell only)' ] = costs_lower[ 'nanopore minion 1 x flowcell (flowcell only)']
    costs_lower['1 x promethion flowcell (flowcell only)' ] = costs_lower[ 'nanopore promethion 1 x flowcell (flowcell only)']
    costs_lower['1 x flongle flowcell (flowcell only)'] = costs_lower[ 'nanopore flongle 1 x flowcell (flowcell only)']
    costs_lower['snp open array (60 snps)' ] = costs_lower[ 'snp open array (60 snps)']
    costs_lower['1 x 36 bp'] = costs_lower['iseq 100 1 x 36 bp']
    costs_lower['1 x 50 bp'] = costs_lower['iseq 100 1 x 50 bp']
    costs_lower['1 x 75 bp'] = costs_lower['iseq 100 1 x 75 bp']
    costs_lower['2 x 75 bp'] = costs_lower['iseq 100 2 x 75 bp']
    costs_lower['2 x 150 bp'] = costs_lower['iseq 100 2 x 150 bp']
    costs_lower['p1 : 1 x 300 bp'] = costs_lower['nextseq2000 1 x 300 bp p1']
    costs_lower['p2 : 2 x 50 bp'] = costs_lower['nextseq2000 2 x 50 bp p2']
    costs_lower['p2 : 2 x 100 bp'] = costs_lower['nextseq2000 2 x 100 bp p2']
    costs_lower['p2 : 2 x 150 bp'] = costs_lower['nextseq2000 2 x 150 bp p2']
    costs_lower['p3 : 1 x 50 bp'] = costs_lower['nextseq2000 1 x 50 bp p3']
    costs_lower['p3 : 2 x 50 bp'] = costs_lower['nextseq2000 2 x 50 bp p3']
    costs_lower['p3 : 2 x 100 bp'] = costs_lower['nextseq2000 2 x 100 bp p3']
    costs_lower['p3 : 2 x 150 bp'] = costs_lower['nextseq2000 2 x 150 bp p3']



    return costs_lower

def getClosestStepCost(all_costs, step ,step_date):
    # print(all_costs)
    step_cost = None
    step_date = datetime.strptime(step_date, "%Y-%m-%d").date()
    # step_costs[ step.name ][step_cost.date]
    for date in sorted(all_costs[step].keys()):
        # date = date.date()
        # print(date.date(), step_date)
        if date.date() <= step_date:
            step_cost = all_costs[step][date]
    if not step_cost:
        # print(step)

        date = sorted(all_costs[step].keys())[-1]
        step_cost = all_costs[step][date]
    return step_cost
    # billing_date = ''
    # step_date = datetime.strptime(step_date, "%Y-%m-%d").date()
    # for date in sorted(all_costs[ step ][ 'date_step_costs'].keys() ):
    #     if date <= step_date:
    #         billing_date = date
    # if not billing_date:
    #     billing_date = sorted(all_costs[ step ][ 'date_step_costs'].keys())[-1]
    # return billing_date

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
    all_costs = getAllCosts()


    step_details = Step(lims, uri=step_uri).details
    runs = {}
    #Get the input artifacts (which is a pool of samples)
    for io_map in step_details.input_output_maps:
        pool = io_map[0]['uri']
        runs[pool.id] = {
        }

        for sample in pool.samples:
            project_id = sample.project.id
            if project_id not in runs[pool.id]:
                runs[pool.id][project_id] = {
                    'errors' : set(),
                    'platform' : None,
                    'name' : None,'id' : None,'open_date' : None,'nr_samples' : 0,'first_submission_date' : None,'received_date' : set(), 'project_comments' : None,#project fields
                    'pool' : pool.id,
                    'lims_runtype' : None,		'requested_runtype' : set(),		'run_personell_costs' : 0, 'run_step_costs':0,		'run_date' : None,			'succesful' : None, #run fields
                    'lims_isolation' : set(),	'type' : set(),						'isolation_personell_costs' : 0,	'isolation_step_costs':0, 'isolation_date' : set(), #isolation fields
                    'lims_library_prep' : set(),'requested_library_prep' : set(),	'libprep_personell_costs' : 0,	'libprep_step_costs':0,'libprep_date' : set(), 'coverages' : [],#libprep fields
                    'lims_analysis' : set(),	'requested_analysis' : set(),		'analysis_personell_costs' : 0,	'analysis_step_costs': 0,'analysis_date' : set(), #analysis fields
                    'total_step_costs' :0,'total_personell_costs':0,
                    'contact_name' : None,'contact_email' : None,'lab_name' : None,'budget_nr' : None,'order_nr' : None,'institute' : None,'postalcode' : None,'city' : None,'country' : None,'department' : None,'street' : None,
                    'vat_nr' : None, 'deb_nr' : None
                }

            runs[pool.id][project_id]['nr_samples'] += 1
            runs[pool.id][project_id]['received_date'].add(sample.date_received)
            runs[pool.id][project_id]['type'].add(sample.udf['Sample Type'])
            if 'Library prep kit' in sample.udf:
                runs[pool.id][project_id]['requested_library_prep'].add(sample.udf['Library prep kit'])

            if 'Sequencing Coverage' in sample.udf:
                runs[pool.id][project_id]['coverages'].append(sample.udf['Sequencing Coverage'])

            runs[pool.id][project_id]['requested_runtype'].add(sample.udf['Sequencing Runtype'])
            runs[pool.id][project_id]['platform'] = sample.udf['Platform']

            sample_artifacts = lims.get_artifacts(samplelimsid=sample.id)

            for sample_artifact in sample_artifacts:

                if not sample_artifact.parent_process: continue
                process_name = sample_artifact.parent_process.type.name
                # print(sample.name, process_name)
                # print(process_name)

                if process_name in Config.ISOLATION_PROCESSES :

                    if sample_artifact.type == 'ResultFile':continue
                    isolation_type = "{0} isolation".format(sample_artifact.udf['US Isolation Type'].split(" ")[0].lower())

                    step_cost = getClosestStepCost(all_costs, isolation_type , sample_artifact.parent_process.date_run)

                    runs[pool.id][project_id]['isolation_step_costs'] += float(step_cost['step_costs'])
                    runs[pool.id][project_id]['isolation_personell_costs'] += float(step_cost['personell_costs'])
                    runs[pool.id][project_id]['total_step_costs']+= float(step_cost['step_costs'])
                    runs[pool.id][project_id]['total_personell_costs']+= float(step_cost['personell_costs'])
                    runs[pool.id][project_id]['lims_isolation'].add(sample_artifact.udf['US Isolation Type'])
                    runs[pool.id][project_id]['isolation_date'].add(sample_artifact.parent_process.date_run)

                    if isolation_type == 'rna isolation' and sample.udf['Sample Type'] != 'RNA unisolated':
                        runs[pool.id][project_id]['errors'].add("Isolation type {0} in LIMS doesn't match sample type {1}".format(isolation_type, sample.udf['Sample Type']))
                    elif isolation_type == 'dna isolation' and sample.udf['Sample Type'] != 'DNA unisolated':
                        runs[pool.id][project_id]['errors'].add("Isolation type {0} in LIMS doesn't match sample type {1}".format(isolation_type, sample.udf['Sample Type']))

                elif process_name in Config.LIBPREP_PROCESSES and sample.udf['Sequencing Runtype'] != 'WGS at HMF' and sample.udf['Sequencing Runtype'] != 'WGS':
                    if sample_artifact.type == 'ResultFile':continue
                # print (sample.project.id, process_name, runs[pool.id]['requested_runtype'])
                    lims_library_prep = ''

                    if 'flongle' in sample.udf['Sequencing Runtype'].lower():
                        lims_library_prep = 'nanopore flongle library prep'
                    elif 'minion' in sample.udf['Sequencing Runtype'].lower() or 'promethion' in sample.udf['Sequencing Runtype'].lower():
                        lims_library_prep = 'nanopore minion library prep'
                    else:
                        protocol_name = getStepProtocol(lims, step_id=sample_artifact.parent_process.id)
                        lims_library_prep = protocol_name.split("-",1)[1].lower().strip()
                        #addition for new protocol names
                        lims_library_prep = lims_library_prep.replace('illumina ', '')

                    runs[pool.id][project_id]['lims_library_prep'].add(lims_library_prep)

                    step_cost = getClosestStepCost(all_costs, lims_library_prep , sample_artifact.parent_process.date_run)

                    runs[pool.id][project_id]['libprep_step_costs'] += float(step_cost['step_costs'])
                    runs[pool.id][project_id]['libprep_personell_costs'] += float(step_cost['personell_costs'])
                    runs[pool.id][project_id]['total_step_costs'] += float(step_cost['step_costs'])
                    runs[pool.id][project_id]['total_personell_costs'] += float(step_cost['personell_costs'])
                    runs[pool.id][project_id]['libprep_date'].add(sample_artifact.parent_process.date_run)


                elif process_name in Config.RUN_PROCESSES and not runs[pool.id][project_id]['lims_runtype']:
                    if sample_artifact.type == 'ResultFile':continue
                    protocol_name = getStepProtocol(lims, step_id=sample_artifact.parent_process.id)
                    runs[pool.id][project_id]['lims_runtype'] = protocol_name.split("-",1)[1].lower().strip()

                    requested_runtype = sample.udf['Sequencing Runtype'].lower()
                    step_cost = getClosestStepCost(all_costs, requested_runtype , sample_artifact.parent_process.date_run)

                    if requested_runtype == 'wgs at hmf' or requested_runtype == 'wgs':

                        runs[pool.id][project_id]['run_date'] = sample_artifact.parent_process.date_run
                    else:
                        runs[pool.id][project_id]['run_step_costs'] = float(step_cost['step_costs'])
                        runs[pool.id][project_id]['run_personell_costs'] = float(step_cost['personell_costs'])
                        runs[pool.id][project_id]['total_step_costs'] += float(step_cost['step_costs'])
                        runs[pool.id][project_id]['total_personell_costs'] += float(step_cost['personell_costs'])
                        runs[pool.id][project_id]['run_date'] = sample_artifact.parent_process.date_run

                elif process_name in Config.ANALYSIS_PROCESSES:

                    runs[pool.id][project_id]['analysis_date'].add(sample_artifact.parent_process.date_run)
                    analysis_steps =['Raw data (FastQ)']
                    analysis_step_costs = 0
                    analysis_personell_costs = 0
                    if sample_artifact.parent_process.udf['Mapping']:
                        analysis_steps.append('Mapping')
                        if sample.udf['Sample Type'].startswith('RNA'):
                            step_cost = getClosestStepCost(all_costs, 'mapping rna' , sample_artifact.parent_process.date_run)
                            analysis_step_costs += float(step_cost['step_costs'])
                            analysis_personell_costs += float(step_cost['personell_costs'])
                        elif 'Targeted' in sample.udf['Analysis']:
                            step_cost = getClosestStepCost(all_costs, 'mapping (targeted selection / chip-seq)' , sample_artifact.parent_process.date_run)
                            analysis_step_costs += float(step_cost['step_costs'])
                            analysis_personell_costs += float(step_cost['personell_costs'])
                        else:
                            step_cost = getClosestStepCost(all_costs, 'mapping wgs' , sample_artifact.parent_process.date_run)
                            analysis_step_costs += float(step_cost['step_costs'])
                            analysis_personell_costs += float(step_cost['personell_costs'])
                    if sample_artifact.parent_process.udf['Germline SNV/InDel calling']:
                        analysis_steps.append('Germline SNV/InDel calling')
                        step_cost = getClosestStepCost(all_costs, 'germline snv/indel calling' , sample_artifact.parent_process.date_run)
                        analysis_step_costs += float(step_cost['step_costs'])
                        analysis_personell_costs += float(step_cost['personell_costs'])
                    if sample_artifact.parent_process.udf['Read count analysis (mRNA)']:
                        analysis_steps.append('Read count analysis (mRNA)')
                        step_cost = getClosestStepCost(all_costs, 'read count analysis (mrna)' , sample_artifact.parent_process.date_run)
                        analysis_step_costs += float(step_cost['step_costs'])
                        analysis_personell_costs += float(step_cost['personell_costs'])
                    if sample_artifact.parent_process.udf['Differential expression analysis + figures (mRNA)']:
                        analysis_steps.append('Differential expression analysis + figures (mRNA)')
                        step_cost = getClosestStepCost(all_costs, 'differential expression analysis + figures (mrna)' , sample_artifact.parent_process.date_run)
                        analysis_step_costs += float(step_cost['step_costs'])
                        analysis_personell_costs += float(step_cost['personell_costs'])
                    if sample_artifact.parent_process.udf['CNV + SV calling']:
                        analysis_steps.append('CNV + SV calling')
                        step_cost = getClosestStepCost(all_costs, 'cnv + sv calling' , sample_artifact.parent_process.date_run)
                        analysis_step_costs += float(step_cost['step_costs'])
                        analysis_personell_costs += float(step_cost['personell_costs'])
                    if sample_artifact.parent_process.udf['Somatic calling (tumor/normal pair)']:
                        analysis_steps.append('Somatic calling (tumor/normal pair)')
                        step_cost = getClosestStepCost(all_costs, 'somatic calling (tumor/normal pair)' , sample_artifact.parent_process.date_run)
                        analysis_step_costs += float(step_cost['step_costs'])
                        analysis_personell_costs += float(step_cost['personell_costs'])

                    runs[pool.id][project_id]['requested_analysis'].add("|".join(sorted(sample.udf['Analysis'].split(","))))
                    runs[pool.id][project_id]['lims_analysis'].add("|".join( sorted( analysis_steps) ))
                    runs[pool.id][project_id]['analysis_step_costs'] += analysis_step_costs
                    runs[pool.id][project_id]['analysis_personell_costs'] += analysis_personell_costs
                    runs[pool.id][project_id]['total_step_costs'] += analysis_step_costs
                    runs[pool.id][project_id]['total_personell_costs'] += analysis_personell_costs
                    if runs[pool.id][project_id]['requested_analysis'] != runs[pool.id][project_id]['lims_analysis']:
                        runs[pool.id][project_id]['errors'].add("Analysis type {0} in LIMS doesn't match analysis {1}".format(runs[pool.id][project_id]['lims_analysis'], runs[pool.id][project_id]['requested_analysis']))

                elif runs[pool.id][project_id]['platform'] == 'Oxford Nanopore' and not runs[pool.id][project_id]['lims_runtype'] and process_name == 'USEQ - Library Pooling':
                #Nanopore fix, since sequencing step does not produce derived samples
                    runs[pool.id][project_id]['lims_runtype'] = sample.udf['Sequencing Runtype'].lower()
                    requested_runtype = sample.udf['Sequencing Runtype'].lower()
                    step_cost = getClosestStepCost(all_costs, requested_runtype , sample_artifact.parent_process.date_run)
                    runs[pool.id][project_id]['run_step_costs'] = float(step_cost['step_costs'])
                    runs[pool.id][project_id]['run_personell_costs'] = float(step_cost['personell_costs'])
                    runs[pool.id][project_id]['total_step_costs'] += float(step_cost['step_costs'])
                    runs[pool.id][project_id]['total_personell_costs'] += float(step_cost['personell_costs'])
                    runs[pool.id][project_id]['run_date'] = sample_artifact.parent_process.date_run
                elif runs[pool.id][project_id]['platform'] == 'Oxford Nanopore' and not runs[pool.id][project_id]['lims_runtype'] and process_name == 'USEQ - Ready for billing':
                #For flowcell only runs on nanopore
                    runs[pool.id][project_id]['lims_runtype'] = sample.udf['Sequencing Runtype'].lower()
                    requested_runtype = sample.udf['Sequencing Runtype'].lower()
                    step_cost = getClosestStepCost(all_costs, requested_runtype , sample.date_received)
                    runs[pool.id][project_id]['run_step_costs'] = float(step_cost['step_costs'])
                    runs[pool.id][project_id]['run_personell_costs'] = float(step_cost['personell_costs'])
                    runs[pool.id][project_id]['total_step_costs'] += float(step_cost['step_costs'])
                    runs[pool.id][project_id]['total_personell_costs'] += float(step_cost['personell_costs'])
                    runs[pool.id][project_id]['run_date'] = sample.date_received


            if not runs[pool.id][project_id]['name'] :
                runs[pool.id][project_id]['first_submission_date'] = sample.date_received

                if 'Sequencing Succesful' in pool.udf :
                    runs[pool.id][project_id]['succesful'] = pool.udf['Sequencing Succesful']

                runs[pool.id][project_id]['name'] = sample.project.name
                runs[pool.id][project_id]['id'] = sample.project.id
                runs[pool.id][project_id]['open_date'] = sample.project.open_date

                if 'Comments and agreements' in sample.project.udf:
                    runs[pool.id][project_id]['project_comments'] = sample.project.udf['Comments and agreements']
                    runs[pool.id][project_id]['project_comments'] = runs[pool.id][project_id]['project_comments'].replace('\n', ' ').replace('\r', '')

                runs[pool.id][project_id]['contact_name'] = sample.project.researcher.first_name + " " + sample.project.researcher.last_name
                runs[pool.id][project_id]['contact_email'] = sample.project.researcher.email
                runs[pool.id][project_id]['lab_name'] = sample.project.researcher.lab.name

                if 'Budget Number' in sample.udf:
                    runs[pool.id][project_id]['budget_nr'] = sample.udf['Budget Number']
                else:
                    print ("No Budgetnumber:", sample.project.id)

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

    for pool in runs:
        # if len(runs[pool].keys()) > 1:
        for pid in runs[pool]:
            if 'WGS' in runs[pool][pid]['requested_runtype'] or 'WGS at HMF' in runs[pool][pid]['requested_runtype']:
                if runs[pool][pid]['coverages']:
                    step_cost = getClosestStepCost(all_costs, list(runs[pool][pid]['requested_runtype'])[0].lower() , runs[pool][pid]['run_date'])
                    for cov in runs[pool][pid]['coverages']:
                        cov = int(cov[:-1])
                        runs[pool][pid]['run_step_costs'] += ( float(step_cost['step_costs']) - 75 ) / (30/ cov ) + 75
                        runs[pool][pid]['run_personell_costs'] = float(0)
                        runs[pool][pid]['total_step_costs'] += ( float(step_cost['step_costs']) - 75 ) / (30/ cov ) + 75
                else:
                    step_cost = getClosestStepCost(all_costs, list(runs[pool][pid]['requested_runtype'])[0].lower() , runs[pool][pid]['run_date'])
                    runs[pool][pid]['run_step_costs'] = float(step_cost['step_costs']) * runs[pool][pid]['nr_samples']
                    runs[pool][pid]['run_personell_costs'] = float(0)
                    runs[pool][pid]['total_step_costs'] += float(step_cost['step_costs']) * runs[pool][pid]['nr_samples']
            else:
                if len(runs[pool].keys()) > 1:
                    runs[pool][pid]['errors'].add('Non-WGS( at HMF) run type in combined sequencing pool!!')


    return renderTemplate('seq_finance_overview_template.csv', {'pools':runs})

def getSnpFinance(lims, step_uri):
    seq_finance = []
    all_costs = getAllCosts()

    step_details = Step(lims, uri=step_uri).details

    runs = {}
    #Get the input artifacts (which is a pool of samples)
    for io_map in step_details.input_output_maps:
        pool = io_map[0]['uri']

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
                    # 'nr_samples' : 0,
                    'samples' : [],
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
            if pool.id + sample.id not in runs[ sample.project.id + budget_nr ]['samples']:
                runs[ sample.project.id + budget_nr ]['samples'].append( pool.id + sample.id )


                runs[sample.project.id + budget_nr]['received_date'].add(sample.date_received)
                runs[sample.project.id + budget_nr]['type'].add(sample.udf['Sample Type'])
                if 'Description' in sample.udf:
                    runs[sample.project.id + budget_nr]['description'].add(sample.udf['Description'])
                step_cost = getClosestStepCost(all_costs, 'open snp array' , sample.date_received)

                runs[sample.project.id + budget_nr]['plate_step_costs'] = float(step_cost['step_costs'])
                runs[sample.project.id + budget_nr]['plate_personell_costs'] = float(step_cost['personell_costs'])

                if sample.udf['Sample Type'] == 'DNA unisolated':
                    step_cost = getClosestStepCost(all_costs, 'dna isolation' , sample.date_received)
                    runs[sample.project.id + budget_nr]['isolation_step_costs'] += float(step_cost['step_costs'])
                    runs[sample.project.id + budget_nr]['total_step_costs'] += float(step_cost['step_costs'])
                    runs[sample.project.id + budget_nr]['isolation_personell_costs'] += float(step_cost['personell_costs'])
                    runs[sample.project.id + budget_nr]['total_personell_costs'] += float(step_cost['personell_costs'])
                elif sample.udf['Sample Type'] == 'RNA unisolated':
                    step_cost = getClosestStepCost(all_costs, 'rna isolation' , sample.date_received)
                    runs[sample.project.id + budget_nr]['isolation_step_costs'] += float(step_cost['step_costs'])
                    runs[sample.project.id + budget_nr]['total_step_costs'] += float(step_cost['step_costs'])
                    runs[sample.project.id + budget_nr]['isolation_personell_costs'] += float(step_cost['personell_costs'])
                    runs[sample.project.id + budget_nr]['total_personell_costs'] += float(step_cost['personell_costs'])

    for id in runs:
        plate_step_costs = runs[id]['plate_step_costs']
        plate_personell_costs = runs[id]['plate_personell_costs']
        # print(id, len(runs[id]['samples']))
        nr_samples = len(runs[id]['samples'])
        runs[id]['total_step_costs'] += (plate_step_costs / 45) * nr_samples
        runs[id]['total_personell_costs'] += (plate_personell_costs / 45) * nr_samples

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
