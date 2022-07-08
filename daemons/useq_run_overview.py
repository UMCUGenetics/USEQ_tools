from config import Config
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


def parseConversionStats( conversion_stats_file ):

    if not Path(conversion_stats_file).is_file():
        return None

    # tree = ET.ElementTree(file=conversion_stats)
    # conversion_stats_file = '/hpc/useq/raw_data/runstats/A00295/210830_A00295_0517_AHJVWFDSX2/ConversionStats.xml'
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

    def getelements(filename_or_file, tag):
        context = iter(ET.iterparse(filename_or_file, events=('start', 'end')))
        _, root = next(context) # get root element
        # print(root)
        for event, elem in context:
            # print(event,elem)
            if event == 'end' and elem.tag == tag:
                yield elem
                root.clear() # preserve memory


    for sample in getelements(conversion_stats_file, "Sample"):
        if sample.attrib['name'] != 'all':continue
        for tile in sample.iter("Tile"):
            # print(sample.attrib['number'],tile.attrib['number'])
            raw_counts = tile.find("./Raw/ClusterCount")
            pf_counts = tile.find("./Pf/ClusterCount")
            pf_read1_counts = tile.find("./Pf/Read[@number='1']")
            pf_read2_counts = tile.find("./Pf/Read[@number='2']")
            conversion_stats['raw_clusters'] += int(raw_counts.text)
            conversion_stats['filtered_clusters'] += int(pf_counts.text)

            r1_yield = pf_read1_counts.find("Yield")
            r1_yield_q30 = pf_read1_counts.find("YieldQ30")
            r1_qsum = pf_read1_counts.find("QualityScoreSum")

            conversion_stats['yield_r1'] += int(r1_yield.text)
            conversion_stats['yield_q30_r1'] += int(r1_yield_q30.text)
            conversion_stats['qsum_r1'] += int(r1_qsum.text)

            if pf_read2_counts is not None:
                r2_yield = pf_read2_counts.find("Yield")
                r2_yield_q30 = pf_read2_counts.find("YieldQ30")
                r2_qsum = pf_read2_counts.find("QualityScoreSum")

                conversion_stats['yield_r2'] += int(r2_yield.text)
                conversion_stats['yield_q30_r2'] += int(r2_yield_q30.text)
                conversion_stats['qsum_r2'] += int(r2_qsum.text)

    conversion_stats['perc_q30_r1'] = (conversion_stats['yield_q30_r1'] / float( conversion_stats['yield_r1']) ) * 100
    conversion_stats['avg_quality_r1'] = conversion_stats['qsum_r1'] / float( conversion_stats['yield_r1'])
    if conversion_stats['yield_r2']:
        conversion_stats['perc_q30_r2'] = (conversion_stats['yield_q30_r2'] / float( conversion_stats['yield_r2']) ) * 100
        conversion_stats['avg_quality_r2'] = conversion_stats['qsum_r2'] / float( conversion_stats['yield_r2'])

    conversion_stats['raw_clusters'] = conversion_stats['raw_clusters']/2
    conversion_stats['filtered_clusters'] = conversion_stats['filtered_clusters']/2

    return conversion_stats

def parseRunSummary( summary ):
    
    stats = {
        'phix_aligned' : [],
        'cluster_density':0,
        'perc_q30' :0,
        'perc_occupied':0
    }
    densities = []
    occupied_scores = []
    q30_scores = []
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
                if len(cols) == 20 and 'nan' not in cols[3]:
                    if cols[10].split("+")[0].rstrip() != 'nan':
                        q30_scores.append(float(cols[10].split("+")[0].rstrip()))
                    if cols[18].split("+")[0].rstrip() != 'nan':
                        occupied_scores.append(float(cols[18].split("+")[0].rstrip()))
                    if cols[3].split("+")[0].rstrip() != 'nan':
                        densities.append(int(cols[3].split("+")[0].rstrip()))
                elif len(cols) == 19 and 'nan' not in cols[3]:
                    if cols[10].split("+")[0].rstrip() != 'nan':
                        q30_scores.append(float(cols[10].split("+")[0].rstrip()))
                    if cols[3].split("+")[0].rstrip() != 'nan':
                        densities.append(int(cols[3].split("+")[0].rstrip()))
                line_nr += 1
            else:
                line_nr += 1

    if len(densities) > 0:
        stats['cluster_density'] = sum(densities) / len(densities)
    else:

        stats['cluster_density'] = None


    if  len(q30_scores) > 0:
        stats['perc_q30'] = sum(q30_scores) / len(q30_scores)
    else:
        stats['perc_q30'] = None

    if len(occupied_scores) > 0:
        stats['perc_occupied'] = sum(occupied_scores) / len(occupied_scores)
    else:
        stats['perc_occupied'] = None
    # if isinstance(stats['perc_occupied'], str):
    #     sys.exit(occupied_scores)
    # print(f"Density : {stats['cluster_density']} Perc Q30 : {stats['perc_q30']} Perc Occupied: {stats['perc_occupied']}")
    return stats

def parseDemuxStats(demux_stats):

    stats = {
        'nr_rows' : 0,
        'total_reads' : 0,
        'perfect_index_reads' : 0,
        'one_mm_index_reads' : 0,
        # 'nr_bases_q30_pf' : 0,
        # 'mean_qual_score_pf' : 0
    }

    with open(demux_stats, 'r') as d:
        csv_reader = csv.DictReader(d)
        for row in csv_reader:
            stats['total_reads'] += float(row['# Reads'])
            stats['perfect_index_reads'] += float(row['# Perfect Index Reads'])
            stats['one_mm_index_reads'] += float(row['# One Mismatch Index Reads'])
            # stats['nr_bases_q30_pf'] += float(row['# of >= Q30 Bases (PF)'])
            # stats['mean_qual_score_pf'] += float(row['Mean Quality Score (PF)'])
            stats['nr_rows'] +=1

    # stats['mean_qual_score_pf'] = stats['mean_qual_score_pf']/stats['nr_rows']

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

def parseQualityMetrics(qual_metrics):
    # print(qual_metrics)
    stats = {
        'avg_quality_r1' : 0,
        'avg_quality_r2' : 0,
        'perc_q30_r1' : 0,
        'perc_q30_r2' : 0
    }
    rows = 0
    with open(qual_metrics, 'r') as q:
        csv_reader = csv.DictReader(q)
        for row in csv_reader:
            if row['ReadNumber'] == '1':
                rows +=1
                stats['avg_quality_r1'] += float(row['Mean Quality Score (PF)'])
                stats['perc_q30_r1'] += float(row['% Q30'])
            elif row['ReadNumber'] == '2':
                stats['avg_quality_r2'] += float(row['Mean Quality Score (PF)'])
                stats['perc_q30_r2'] += float(row['% Q30'])
    # print(rows)
    stats['avg_quality_r1'] = stats['avg_quality_r1']/rows
    stats['perc_q30_r1'] = stats['perc_q30_r1']/rows
    stats['avg_quality_r2'] = stats['avg_quality_r2']/rows
    stats['perc_q30_r2'] = stats['perc_q30_r2']/rows

    return stats
#
# run['avg_quality_r1'] = qual_metrics['avg_quality_r1']
# run['avg_quality_r2'] = qual_metrics['avg_quality_r2']
# run['perc_bases_q30_r1'] = qual_metrics['perc_q30_r1']
# run['perc_bases_q30_r2'] = qual_metrics['perc_q30_r2']

def getProjectDetails( lims, project,run_dirs ):
    # print(run_dirs['AHKHMNBGXK'])
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
    # print (f'Getting samples start {datetime.now()}'  )
    samples = lims.get_samples(projectlimsid=project.id)
    # print (f'Getting samples stop {datetime.now()}'  )
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


    process_types = []
    #Configured steps plus some legacy stuff
    ILL_SEQUENCING_STEPS = Config.WORKFLOW_STEPS['SEQUENCING']['steps']['ILLUMINA SEQUENCING']['names'] + ['NextSeq Run (NextSeq) 1.0','MiSeq Run (MiSeq) 4.0','HiSeq Run (HiSeq) 5.0']
    NAN_SEQUENCING_STEPS = Config.WORKFLOW_STEPS['SEQUENCING']['steps']['NANOPORE SEQUENCING']['names']

    process_types.extend(ILL_SEQUENCING_STEPS)
    process_types.extend(NAN_SEQUENCING_STEPS)
    process_types.extend(['USEQ - BCL to FastQ','USEQ - Ready for billing','USEQ - Process Raw Data'])
    project_processes = lims.get_processes(type=process_types, projectname=project.name)

    # print (f'Getting processes stop {datetime.now()}' )
    if not project_processes:
        return project_details

    for process_nr, process in enumerate(project_processes):
        # print (f'Getting processes start 1{process.type.name} {datetime.now()}' )
        # print (process)

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
                'perc_occupied' : None,
                'perc_bases_q30' : None,
                'perc_bases_q30_r1' : None,
                'perc_bases_q30_r2' : None,
                'cluster_density' : None,
                'sequencing_succesful' : None,
                'phix_aligned_r1' : None,
                'phix_aligned_r2' : None,
                'loading_conc_pm' : getUDF(process.input_output_maps[0][0]['uri'],"Loading Conc. (pM)" )

            }
            # print(process.type.name, process_nr, project_processes)
            if (process_nr + 1) < len(project_processes):
                for io in project_processes[process_nr + 1].input_output_maps:

                    if samples[0].id == io[0]['uri'].samples[0].id and io[1]:

                        run['sequencing_succesful'] = getUDF(io[1]['uri'], 'Sequencing Succesful')
                        run['data_send'] = project_processes[process_nr + 1].date_run
                        break

            # print(run_dirs)
            if run['flowcell'] in run_dirs:
                # print('test',run['flowcell'])
                conversion_stats_file = Path(f"{run_dirs[run['flowcell']]}/ConversionStats.xml")
                demux_stats_file = Path(f"{run_dirs[run['flowcell']]}/Demultiplex_Stats.csv")
                qual_metrics_file = Path(f"{run_dirs[run['flowcell']]}/Quality_Metrics.csv")
                adapter_metrics_file = Path(f"{run_dirs[run['flowcell']]}/Adapter_Metrics.csv")
                summary_stats_file = Path(f"{run_dirs[run['flowcell']]}/{ Path(run_dirs[run['flowcell']]).name }_summary.csv")
                # print(summary_stats_file)
                run['name'] = Path(run_dirs[run['flowcell']]).name
                if conversion_stats_file.is_file():
                    conversion_stats = parseConversionStats(conversion_stats_file)

                    run['raw_clusters'] = conversion_stats['raw_clusters']
                    run['filtered_clusters'] = conversion_stats['filtered_clusters']
                    run['avg_quality_r1'] = conversion_stats['avg_quality_r1']
                    run['avg_quality_r2'] = conversion_stats['avg_quality_r2']
                    run['perc_bases_q30_r1'] = conversion_stats['perc_q30_r1']
                    run['perc_bases_q30_r2'] = conversion_stats['perc_q30_r2']

                elif demux_stats_file.is_file():
                    # Lane,SampleID,Index,# Reads,# Perfect Index Reads,# One Mismatch Index Reads,# of >= Q30 Bases (PF),Mean Quality Score (PF)
                    demux_stats = parseDemuxStats(demux_stats_file)
                    run['filtered_clusters'] = int(demux_stats['total_reads'])

                    if qual_metrics_file.is_file():
                        qual_metrics = parseQualityMetrics(qual_metrics_file)
                        run['avg_quality_r1'] = qual_metrics['avg_quality_r1']
                        run['avg_quality_r2'] = qual_metrics['avg_quality_r2']
                        run['perc_bases_q30_r1'] = qual_metrics['perc_q30_r1']
                        run['perc_bases_q30_r2'] = qual_metrics['perc_q30_r2']
                    # run['avg_quality'] = demux_stats['mean_qual_score_pf']


                    # adapter_stats = parseAdapterMetrics(adapter_metrics_file)
                    # run['perc_bases_q30'] = (demux_stats['nr_bases_q30_pf']/adapter_stats['total_bases'])*100

                if summary_stats_file.is_file():
                    summary_stats = parseRunSummary(summary_stats_file)
                    # print(summary_stats)
                    run['cluster_density'] = summary_stats['cluster_density']
                    run['perc_bases_q30'] = summary_stats['perc_q30']
                    run['perc_occupied'] = summary_stats['perc_occupied']
                    run['phix_aligned_r1'] = summary_stats['phix_aligned'][0]
                    if len(summary_stats['phix_aligned']) > 1: run['phix_aligned_r2'] = summary_stats['phix_aligned'][1]

            project_details['runs'].append(run)
            # print (project_details['runs'])
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
                'perc_occupied' : None,
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

        # print (f'Getting processes stop {process.type.name} {datetime.now()}' )
    project_details['times_sequenced'] = len(project_details['runs'])


    return project_details



def loadProjectOverview( ovw_file ):
    ovw = {}

    with open(ovw_file, 'r') as in_file:
        ovw = json.load(in_file)

    return ovw


def updateProjectOverview( lims, ovw ):
    # print('Retrieving projects')
    projects = lims.get_projects()

    new_ovw = {}

    run_dirs = {}
    p = Path(Config.HPC_STATS_DIR)
    # print('Getting run directories')
    # print(p)
    for d in p.glob('*/*'):

        flowcell = d.name.split("_")[-1].split("-")[-1]
        # print(flowcell)
        # print(flowcell[1:])
        # print(d)
        run_dirs[flowcell] = d
        run_dirs[flowcell[1:]] = d
    # c = 0
    # print('Processing projects')
    # for pr in projects[::-1]:
    for pr in projects:

        try:
            application = pr.udf['Application']
            print (pr,application)
            if application not in Config.PROJECT_TYPES.values():
                continue
        except KeyError:
            continue

        # print (f"Working on {pr.id}")
        if pr.id in ovw:
            if not ovw[pr.id]['close-date'] :
                # print('really')
                new_ovw[pr.id] = getProjectDetails(lims, pr,run_dirs)
            else:
                new_ovw[pr.id] = ovw[pr.id]
        else:
            new_ovw[pr.id] = getProjectDetails(lims, pr,run_dirs)
            # print('test',new_ovw[pr.id])
        # c+=1
        # # print (c)
        # if c==5:
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
                    'avg_quality' : run.get('avg_quality',None),
                    'avg_quality_r1' : run['avg_quality_r1'],
                    'avg_quality_r2' : run['avg_quality_r2'],
                    'perc_occupied' : run.get('perc_occupied', None),
                    'perc_bases_q30' : run.get('perc_bases_q30',None),
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
                'perc_occupied' : None,
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
    # project_ovw = loadProjectOverview( overview_file )

    # #update project info from non-closed projects
    project_ovw = updateProjectOverview( lims, project_ovw )

    # #write new project info overview
    writeProjectOverview( project_ovw, overview_file )
