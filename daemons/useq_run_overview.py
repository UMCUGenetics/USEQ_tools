from config import PROJECT_TYPES,ILL_SEQUENCING_STEPS,NAN_SEQUENCING_STEPS,STAT_PATH
import argparse
import json
import xml.etree.cElementTree as ET
import csv
from pathlib import Path
from datetime import datetime

def getAdapterSet( lims, artifact ):
    adapter_set = None

    try:
        adapter_set = lims.get_reagent_types(artifact.reagent_labels[0])[0].category
    except:
        adapter_set = None

    return adapter_set

def getUDF(entity, udf_name):
    try:
        return str(entity.udf[udf_name])
    except:
        return None

def getBillingComments(lims, process, project):


    for io in process.input_output_maps:
        output = io[1]
        if output['output-type'] == 'Analyte':
            if output['uri'].samples[0].id == project.id + 'A1':
                return getUDF(output['uri'],"Comment field")

def parseConversionStats( conversion_stats ):

    if not Path(conversion_stats).is_file():
        return None

    tree = ET.ElementTree(file=conversion_stats)
    conversion_stats = {
        'raw_clusters' : 0, #NA
        'filtered_clusters' : 0, # = # Reads
        'yield_r1' : 0, #NA
        'yield_r2' : 0, #NA
        'yield_q30_r1' : 0,#NA
        'yield_q30_r2' : 0,#NA
        'qsum_r1' : 0,#NA
        'qsum_r2' : 0,#NA
        'avg_quality_r1' : 0,#NA
        'avg_quality_r2' : 0,#NA
        'perc_q30_r1' : 0,#NA
        'perc_q30_r2' : 0,#NA
        'bases_r1' : 0,
        'bases_r2' : 0,
        'perc_q30' : 0,
        'perc_perfect_index' :0,
        'mean_quality' : 0
    }
    for project in tree.iter(tag='Project'):
        if project.attrib['name'] != 'all': continue
        for tile in project.iter(tag='Tile'):
            raw_counts = tile.find('Raw')
            pf_counts = tile.find('Pf')
            conversion_stats['raw_clusters'] += int(raw_counts.find("ClusterCount").text)
            conversion_stats['filtered_clusters'] += int(pf_counts.find("ClusterCount").text)
            for read in pf_counts.findall('Read'):
                read_nr = int(read.attrib['number'])
                if read_nr > 2: continue
                conversion_stats[f'yield_r{read_nr}'] += int(read.find("Yield").text)
                conversion_stats[f'yield_q30_r{read_nr}'] += int(read.find("YieldQ30").text)
                conversion_stats[f'qsum_r{read_nr}'] += int(read.find("QualityScoreSum").text)

    conversion_stats['perc_q30_r1'] = (conversion_stats['yield_q30_r1'] / float( conversion_stats['yield_r1']) ) * 100
    conversion_stats['avg_quality_r1'] = conversion_stats['qsum_r1'] / float( conversion_stats['yield_r1'])
    if conversion_stats['yield_r2']:
        conversion_stats['perc_q30_r2'] = (conversion_stats['yield_q30_r2'] / float( conversion_stats['yield_r2']) ) * 100
        conversion_stats['avg_quality_r2'] = conversion_stats['qsum_r2'] / float( conversion_stats['yield_r2'])


    return conversion_stats

def parseRunSummary( summary ):
    stats = {'phix_aligned' : [],'cluster_density':0}
    densities = []
    with open(summary, 'r') as sumcsv:
        lines = sumcsv.readlines()
        line_nr = 0
        # for line_nr in range(len(lines)):
        while line_nr < len(lines):
            line = lines[line_nr].rstrip()
            if not line: line_nr+=1;continue
            if line.startswith('Level'):
                for sub_line in lines[line_nr+1:line_nr+5]:
                    # print (line)
                    cols = sub_line.split(",")
                    if cols[0].startswith('Read') and not cols[0].rstrip().endswith('(I)'):
                        # print (sub_line)
                        stats['phix_aligned'].append(float(cols[3].rstrip()))
                line_nr+=5

            elif line[0].isdigit():
                cols = line.split(",")
                # print(cols)
                if len(cols) > 4 and 'nan' not in cols[3]:

                    densities.append(int(cols[3].split("+")[0].rstrip()))
                line_nr += 1
            else:
                line_nr += 1
    #print(densities)
    stats['cluster_density'] = sum(densities) / len(densities)
    return stats

def parseDemuxStats(demux_stats):

    stats = {
        'nr_rows' : 0,
        'total_reads' : 0,
        'perfect_index_reads' : 0,
        'one_mm_index_reads' : 0,
        'nr_bases_q30_pf' : 0,
        'mean_qual_score_pf' : 0
    }

    with open(demux_stats, 'r') as d:
        csv_reader = csv.DictReader(d)
        for row in csv_reader:
            stats['total_reads'] += float(row['# Reads'])
            stats['perfect_index_reads'] += float(row['# Perfect Index Reads'])
            stats['one_mm_index_reads'] += float(row['# One Mismatch Index Reads'])
            stats['nr_bases_q30_pf'] += float(row['# of >= Q30 Bases (PF)'])
            stats['mean_qual_score_pf'] += float(row['Mean Quality Score (PF)'])
            stats['nr_rows'] +=1

    stats['mean_qual_score_pf'] = stats['mean_qual_score_pf']/stats['nr_rows']

    return stats

def parseAdapterMetrics(adapter_metrics):
    stats = {
        'r1_bases' : 0,
        'r2_bases' : 0,
        'total_bases' : 0

    }
    with open(adapter_metrics, 'r') as a:
        csv_reader = csv.DictReader(a)
        for row in csv_reader:
            stats['r1_bases'] += float(row['R1_SampleBases'])
            stats['total_bases'] +=float(row['R1_SampleBases'])
            if 'R2_SampleBases' in row:
                stats['r2_bases'] += float(row['R2_SampleBases'])
                stats['total_bases'] += float(row['R2_SampleBases'])
    return stats

def getProjectDetails( lims, project,run_dirs ):
    project_details = {}
    project_details['runs'] = []

    project_details['application'] = project.udf['Application']
    project_details['name'] =  project.name
    project_details['id'] = project.id
    project_details['open-date'] = project.open_date
    project_details['close-date'] = project.close_date
    project_details['library_prep_nr'] = None
    project_details['isolation_nr'] = None
    project_details['sample_nr'] = None
    project_details['sample_type'] = None
    project_details['library_prep'] = None
    project_details['run_type'] = None
    project_details['billing_comments'] = None
    project_details['times_sequenced'] = None
    project_details['samples_submitted'] = None
    project_details['samples_arrived'] = getUDF(project,'Samples Arrived')
    project_details['priority'] = getUDF(project,'Priority')
    project_details['comments'] = getUDF(project,'Comments and agreements')
    project_details['consent'] = None
    project_details['adapter_set'] = None
    project_details['researcher_id'] = project.researcher.id #ADDITION
    project_details['researcher_name'] = f'{project.researcher.first_name} {project.researcher.last_name}' #ADDITION
    project_details['lab_id'] = project.researcher.lab.id #ADDITION
    project_details['lab_name'] = project.researcher.lab.name
    project_details['budget_number'] = None
    print (f'Getting samples start {datetime.now()}'  )
    samples = lims.get_samples(projectlimsid=project.id)
    print (f'Getting samples stop {datetime.now()}'  )
    if len(samples) == 0:
        return project_details

    project_details['sample_nr'] = len(samples)
    project_details['sample_type'] = getUDF(samples[0],'Sample Type')
    if not project_details['sample_type']: return project_details
    project_details['library_prep'] = getUDF(samples[0],'Library prep kit')
    project_details['run_type'] = getUDF(samples[0],'Sequencing Runtype')
    project_details['samples_submitted'] = samples[0].date_received
    project_details['consent'] = getUDF(samples[0],'Informed Consent')
    project_details['budget_number'] = getUDF(samples[0],'Budget Number')
    project_details['adapter_set'] = getAdapterSet(lims, samples[0].artifact)

    if project_details['sample_type'].endswith("unisolated"):
        project_details['library_prep_nr'] = project_details['sample_nr']
        project_details['isolation_nr'] = project_details['sample_nr']
    elif project_details['sample_type'].endswith("isolated"):
        project_details['library_prep_nr'] = project_details['sample_nr']

    print (f'Getting processes start {datetime.now()}' )
    project_processes = lims.get_processes(projectname=project.name)

    print (f'Getting processes stop {datetime.now()}' )
    if not project_processes:
        return project_details

    for process_nr, process in enumerate(project_processes):
        print (f'Getting processes start 1{process.type.name} {datetime.now()}' )
        if process.type.name in ILL_SEQUENCING_STEPS:
            run = {
                'name' : 'NA',
                'flowcell' : getUDF(process.parent_processes()[0],'Flow Cell ID') if process.type.name == 'USEQ - Automated NovaSeq Run' else getUDF(process, 'Flow Cell ID'),
                'run_started' : process.date_run,
                'data_send': None,
                'phix': getUDF(process.parent_processes()[0],'% PhiX Control'),
                'sequencing_succesful': None,
                'raw_clusters' : None,  #total_reads_raw
                'filtered_clusters' : None, #total_reads
                'avg_quality' : None,
                'avg_quality_r1' : None,
                'avg_quality_r2' : None,
                'perc_bases_q30' : None,
                'perc_bases_q30_r1' : None,
                'perc_bases_q30_r2' : None,
                'cluster_density' : None,
                'sequencing_succesful' : None,
                'phix_aligned_r1' : None,
                'phix_aligned_r2' : None,
                'loading_conc_pm' : getUDF(process.input_output_maps[0][0]['uri'],"Loading Conc. (pM)" )

            }

            if (process_nr + 1) < len(project_processes):
                for io in project_processes[process_nr + 1].input_output_maps:
                    if samples[0].id == io[0]['uri'].samples[0].id:
                        run['sequencing_succesful'] = getUDF(io[1]['uri'], 'Sequencing Succesful')
                        run['data_send'] = project_processes[process_nr + 1].date_run
                        break

            if run['flowcell'] in run_dirs:
                conversion_stats_file = Path(f"{run_dirs[run['flowcell']]}/ConversionStats.xml")
                demux_stats_file = Path(f"{run_dirs[run['flowcell']]}/Demultiplex_Stats.csv")
                adapter_metrics_file = Path(f"{run_dirs[run['flowcell']]}/Adapter_Metrics.csv")
                summary_stats_file = Path(f"{run_dirs[run['flowcell']]}/{ Path(run_dirs[run['flowcell']]).name }_summary.csv")
                # print(summary_stats_file)
                if conversion_stats_file.is_file():
                    conversion_stats = parseConversionStats(conversion_stats_file)

                    run['name'] = Path(run_dirs[run['flowcell']]).name
                    run['raw_clusters'] = conversion_stats['raw_clusters']
                    run['filtered_clusters'] = conversion_stats['filtered_clusters']
                    run['avg_quality_r1'] = conversion_stats['avg_quality_r1']
                    run['avg_quality_r2'] = conversion_stats['avg_quality_r2']
                    run['perc_bases_q30_r1'] = conversion_stats['perc_q30_r1']
                    run['perc_bases_q30_r2'] = conversion_stats['perc_q30_r2']

                if demux_stats_file.is_file() and adapter_metrics_file.is_file():
                    # Lane,SampleID,Index,# Reads,# Perfect Index Reads,# One Mismatch Index Reads,# of >= Q30 Bases (PF),Mean Quality Score (PF)
                    demux_stats = parseDemuxStats(demux_stats_file)
                    run['filtered_clusters'] = demux_stats['total_reads']
                    run['avg_quality'] = demux_stats['mean_qual_score_pf']


                    adapter_stats = parseAdapterMetrics(adapter_metrics_file)
                    run['perc_bases_q30'] = (demux_stats['nr_bases_q30_pf']/adapter_stats['total_bases'])*100

                if summary_stats_file.is_file():
                    summary_stats = parseRunSummary(summary_stats_file)
                    run['cluster_density'] = summary_stats['cluster_density']
                    run['phix_aligned_r1'] = summary_stats['phix_aligned'][0]
                    if len(summary_stats['phix_aligned']) > 1: run['phix_aligned_r2'] = summary_stats['phix_aligned'][1]

            project_details['runs'].append(run)
            print (project_details['runs'])
        elif process.type.name in NAN_SEQUENCING_STEPS:
            # continue
            run = {
                'name' : project.name,
                'flowcell' : None,
                'run_started' : process.date_run,
                'data_send' : None,
                'phix': None,
                'loading_conc_pm': None,
                'sequencing_succesful': None,
                'raw_clusters' : None,
                'filtered_clusters' : None,
                'avg_quality' : None,
                'avg_quality_r1' : None,
                'avg_quality_r2' : None,
                'perc_bases_q30' : None,
                'perc_bases_q30_r1' : None,
                'perc_bases_q30_r2' : None,
                'cluster_density' : None,
                'sequencing_succesful' : None,
                'phix_aligned_r1' : None,
                'phix_aligned_r2' : None,
                'loading_conc_pm' : None

            }
            if (process_nr + 1) < len(project_processes): #Trying to find the result of the BCL to FASTQ step
                for io in project_processes[process_nr + 1].input_output_maps:
                    if samples[0].id == io[0]['uri'].samples[0].id and io[1]:

                        run['sequencing_succesful'] = getUDF(io[1]['uri'], 'Sequencing Succesful')
                        run['data_send'] = project_processes[process_nr + 1].date_run
                        # print (run['data_send'])
                        break
            project_details['runs'].append(run)
        elif process.type.name == 'USEQ - Ready for billing':
            project_details['billing_comments'] = getBillingComments(lims, process, project)

        print (f'Getting processes stop {process.type.name} {datetime.now()}' )
    project_details['times_sequenced'] = len(project_details['runs'])


    return project_details



def loadProjectOverview( ovw_file ):
    ovw = {}

    with open(ovw_file, 'r') as in_file:
        ovw = json.load(in_file)

    return ovw


def updateProjectOverview( lims, ovw ):
    projects = lims.get_projects()
    new_ovw = {}

    run_dirs = {}
    p = Path(STAT_PATH)
    for d in p.glob('*/*'):

        flowcell = d.name.split("_")[-1].split("-")[-1]
        run_dirs[flowcell] = d
        run_dirs[flowcell[1:]] = d

    for pr in projects[::-1]:
        try:
            application = pr.udf['Application']
            # print (pr,application)
            if application not in PROJECT_TYPES:
                continue
        except KeyError:
            continue

        print (f"Working on {pr.id} ")
        if pr.id in ovw:
            if not ovw[pr.id]['close-date'] :
                # print ('really working on it')
                new_ovw[pr.id] = getProjectDetails(lims, pr,run_dirs)
            else:
                new_ovw[pr.id] = ovw[pr.id]
        else:
            new_ovw[pr.id] = getProjectDetails(lims, pr,run_dirs)
            print(new_ovw[pr.id])
        # c+=1
        # # print (c)
        # if c==50:
        #     break
    return new_ovw

def writeProjectOverview( ovw, ovw_file ):
    ovw_per_run = []
    for pid in sorted(ovw):
        if ovw[pid]['runs']:
            for run in ovw[pid]['runs']:
                ovw_per_run.append(
                    {
                    'run_name' : run['name'],
                    'run_started' : run['run_started'],
                    'data_send': run['data_send'],
                    'phix': run['phix'],
                    'sequencing_succesful': run['sequencing_succesful'],
                    'raw_clusters' : run['raw_clusters'],
                    'filtered_clusters' : run['filtered_clusters'],
                    'avg_quality' : run['avg_quality'],
                    'avg_quality_r1' : run['avg_quality_r1'],
                    'avg_quality_r2' : run['avg_quality_r2'],
                    'perc_bases_q30' : run['perc_bases_q30'],
                    'perc_bases_q30_r1' : run['perc_bases_q30_r1'],
                    'perc_bases_q30_r2' : run['perc_bases_q30_r2'],
                    'cluster_density' : run['cluster_density'],
                    'phix_aligned_r1' : run['phix_aligned_r1'],
                    'phix_aligned_r2' : run['phix_aligned_r2'],
                    'loading_conc_pm' : run['loading_conc_pm'],
                    'lab' : ovw[pid]['lab_name'],
                    'lab_id' : ovw[pid]['lab_id'],
                    'researcher' : ovw[pid]['researcher_name'],
                    'researcher_id' : ovw[pid]['researcher_id'],
                    'budget_number' : ovw[pid]['budget_number'],
                    'name' : ovw[pid]['name'],
                    'id' : ovw[pid]['id'],
                    'open-date' : ovw[pid]['open-date'],
                    'close-date' : ovw[pid]['close-date'],
                    'application' : ovw[pid]['application'],
                    'priority' : ovw[pid]['priority'],
                    'consent' : ovw[pid]['consent'],
                    'samples_arrived' : ovw[pid]['samples_arrived'],
                    'samples_submitted' : ovw[pid]['samples_submitted'],
                    'sample_type' : ovw[pid]['sample_type'],
                    'sample_nr' : ovw[pid]['sample_nr'],
                    'library_prep' : ovw[pid]['library_prep'],
                    'library_prep_nr' : ovw[pid]['library_prep_nr'],
                    'isolation_nr' : ovw[pid]['isolation_nr'],
                    'adapter_set' : ovw[pid]['adapter_set'],
                    'run_type' : ovw[pid]['run_type'],
                    'comments' : ovw[pid]['comments'],
                    'billing_comments' : ovw[pid]['billing_comments']
                    }
                )
        else:
            ovw_per_run.append(
                {
                'run_name' : None,
                'run_started' : None,
                'data_send': None,
                'phix': None,
                'sequencing_succesful': None,
                'raw_clusters' : None,
                'filtered_clusters' : None,
                'avg_quality' : None,
                'avg_quality_r1' : None,
                'avg_quality_r2' : None,
                'perc_bases_q30' : None,
                'perc_bases_q30_r1' : None,
                'perc_bases_q30_r2' : None,
                'cluster_density' : None,
                'phix_aligned_r1' : None,
                'phix_aligned_r2' : None,
                'loading_conc_pm' : None,
                'lab' : ovw[pid]['lab_name'],
                'lab_id' : ovw[pid]['lab_id'],
                'researcher' : ovw[pid]['researcher_name'],
                'researcher_id' : ovw[pid]['researcher_id'],
                'budget_number' : ovw[pid]['budget_number'],
                'name' : ovw[pid]['name'],
                'id' : ovw[pid]['id'],
                'open-date' : ovw[pid]['open-date'],
                'close-date' : ovw[pid]['close-date'],
                'application' : ovw[pid]['application'],
                'priority' : ovw[pid]['priority'],
                'consent' : ovw[pid]['consent'],
                'samples_arrived' : ovw[pid]['samples_arrived'],
                'samples_submitted' : ovw[pid]['samples_submitted'],
                'sample_type' : ovw[pid]['sample_type'],
                'sample_nr' : ovw[pid]['sample_nr'],
                'library_prep' : ovw[pid]['library_prep'],
                'library_prep_nr' : ovw[pid]['library_prep_nr'],
                'isolation_nr' : ovw[pid]['isolation_nr'],
                'adapter_set' : ovw[pid]['adapter_set'],
                'run_type' : ovw[pid]['run_type'],
                'comments' : ovw[pid]['comments'],
                'billing_comments' : ovw[pid]['billing_comments']
                }
            )

    ovw_file_web = ovw_file.split(".")[0] + "_web.json"


    with open(ovw_file, 'w') as out_file:
        json.dump(ovw, out_file,sort_keys=True,indent=4, separators=(',', ': '))

    with open(ovw_file_web, 'w') as out_file:
        json.dump(ovw_per_run, out_file, sort_keys=True,indent=4, separators=(',', ': '))






def run(lims, overview_file):

    project_ovw = []
    #get project info from existing overview
    project_ovw = loadProjectOverview( overview_file )

    # #update project info from non-closed projects
    project_ovw = updateProjectOverview( lims, overview_file )

    # #write new project info overview
    writeProjectOverview( project_ovw, overview_file )
