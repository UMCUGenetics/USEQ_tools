"""Finance overview generation for sequencing and SNP fingerprinting runs.

This module calculates costs for sequencing runs and generates billing reports
by fetching project costs from the portal API and combining with LIMS data.
"""

import json
import re
import sys
from typing import Dict, List, Any, Set, Tuple, Optional

import requests
from genologics.entities import Step, ProtocolStep

from config import Config
from modules.useq_template import renderTemplate


class FinanceError(Exception):
    """Custom exception for finance-related errors."""
    pass


class CostCalculationError(FinanceError):
    """Exception for cost calculation errors."""
    pass


def get_step_protocol(lims, step_id: Optional[str] = None,
                     step_uri: Optional[str] = None) -> str:
    """Get protocol name from step ID or URI.

    Args:
        lims: LIMS connection
        step_id: Optional step ID
        step_uri: Optional step URI

    Returns:
        Protocol name

    Raises:
        ValueError: If neither step_id nor step_uri is provided
    """
    if not step_id and not step_uri:
        raise ValueError("Either step_id or step_uri must be provided")

    if step_uri:
        step_config = Step(lims, uri=step_uri).configuration
    else:
        step_config = Step(lims, id=step_id).configuration

    protocol_uri = re.sub(r"/steps/\d+", "", step_config.uri)
    return ProtocolStep(lims, uri=protocol_uri).name


def initialize_project_run_data(pool_id: str) -> Dict[str, Any]:
    """Initialize run data structure for a project.

    Args:
        pool_id: Pool identifier

    Returns:
        Initialized run data dictionary
    """
    return {
        'errors': set(),
        'platform': None,
        'application': None,
        'name': None,
        'id': None,
        'open_date': None,
        'nr_samples_submitted': 0,
        'nr_samples_isolated': 0,
        'nr_samples_prepped': 0,
        'nr_samples_sequenced': 0,
        'nr_samples_analyzed': 0,
        'first_submission_date': None,
        'received_date': set(),
        'project_comments': None,
        'times_sequenced': 0,
        'pool': pool_id,
        'lims_runtype': None,
        'requested_runtype': set(),
        'run_personell_costs': 0,
        'run_step_costs': 0,
        'run_date': None,
        'run_lanes': None,
        'succesful': None,
        'lims_isolation': set(),
        'type': set(),
        'isolation_personell_costs': 0,
        'isolation_step_costs': 0,
        'isolation_date': set(),
        'lims_library_prep': set(),
        'requested_library_prep': set(),
        'libprep_personell_costs': 0,
        'libprep_step_costs': 0,
        'libprep_date': set(),
        'lims_analysis': set(),
        'requested_analysis': set(),
        'analysis_personell_costs': 0,
        'analysis_step_costs': 0,
        'analysis_date': set(),
        'total_step_costs': 0,
        'total_personell_costs': 0,
        'contact_name': None,
        'contact_email': None,
        'lab_name': None,
        'budget_nr': None,
        'order_nr': None,
        'institute': None,
        'postalcode': None,
        'city': None,
        'country': None,
        'department': None,
        'street': None,
        'vat_nr': None,
        'deb_nr': None
    }


def initialize_sample_metadata() -> Dict[str, Any]:
    """Initialize sample metadata structure.

    Returns:
        Initialized sample metadata dictionary
    """
    return {
        'Sample Type': '',
        'Analysis': '',
        'Sequencing Coverage': 0,
        'Reads': 0,
        'Add-ons': [],
        'Isolated': False,
        'Isolated_date': None,
        'Prepped': False,
        'Prepped_date': None,
        'Sequenced': False,
        'Analyzed': False,
        'Analyzed_date': None
    }


def initialize_run_metadata() -> Dict[str, Any]:
    """Initialize run metadata structure for cost calculation.

    Returns:
        Initialized run metadata dictionary
    """
    return {
        'Application': None,
        'Platform': None,
        'Sequencing Runtype': None,
        'Library prep kit': None,
        'Number Lanes': None,
        'date': None,
        'samples': []
    }


def normalize_application_platform(application: str, platform: str) -> Tuple[str, str]:
    """Normalize legacy application and platform names.

    Args:
        application: Application name
        platform: Platform name

    Returns:
        Tuple of normalized (application, platform)
    """
    # Handle legacy application names
    if application == 'USF - Sequencing':
        if platform == 'Oxford Nanopore':
            application = 'ONT Sequencing'
        elif 'Illumina' in platform:
            application = 'Illumina Sequencing'
        elif 'Chromium' in platform:
            application = '10X Single Cell'
            platform = 'Chromium X'
    elif application in ['USF - SNP genotyping', 'SNP Fingerprinting']:
        application = 'SNP Fingerprinting'
        if platform not in ['SNP Fingerprinting', '60 SNP NimaGen panel']:
            platform = '60 SNP NimaGen panel'

    # Remove 'Illumina ' prefix from platform
    platform = platform.replace('Illumina ', '')

    return application, platform


def process_isolation_artifact(sample_artifact, sample_meta: Dict[str, Any],
                              run_data: Dict[str, Any]) -> None:
    """Process isolation artifact and update metadata.

    Args:
        sample_artifact: Artifact from isolation process
        sample_meta: Sample metadata dictionary
        run_data: Run data dictionary
    """
    if sample_artifact.type == 'ResultFile':
        return

    isolation_type = f"{sample_artifact.udf['US Isolation Type'].split()[0].lower()} isolation"

    if not sample_meta['Isolated']:
        run_data['nr_samples_isolated'] += 1
        sample_meta['Isolated_date'] = sample_artifact.parent_process.date_run
        sample_meta['Isolated'] = True

    run_data['lims_isolation'].add(sample_artifact.udf['US Isolation Type'])
    run_data['isolation_date'].add(sample_artifact.parent_process.date_run)

    # Validate isolation type matches sample type
    sample_type = sample_meta['Sample Type']
    if isolation_type == 'rna isolation' and sample_type != 'RNA unisolated':
        run_data['errors'].add(
            f"Warning: Isolation type {isolation_type} in LIMS doesn't match sample type {sample_type}"
        )
    elif isolation_type == 'dna isolation' and sample_type != 'DNA unisolated':
        run_data['errors'].add(
            f"Warning: Isolation type {isolation_type} in LIMS doesn't match sample type {sample_type}"
        )


def process_libprep_artifact(lims, sample, sample_artifact,
                            sample_meta: Dict[str, Any],
                            run_data: Dict[str, Any]) -> None:
    """Process library prep artifact and update metadata.

    Args:
        lims: LIMS connection
        sample: Sample object
        sample_artifact: Artifact from library prep process
        sample_meta: Sample metadata dictionary
        run_data: Run data dictionary
    """
    if sample_artifact.type == 'ResultFile':
        return



    runtype = sample.udf['Sequencing Runtype']
    lims_library_prep = ''

    if 'flongle' in runtype.lower():
        lims_library_prep = 'nanopore flongle library prep'
    elif 'minion' in runtype.lower() or 'promethion' in runtype.lower():
        lims_library_prep = 'nanopore minion library prep'
    elif 'gem-x' in runtype.lower():
        lims_library_prep = runtype.lower()
    else:
        protocol_name = get_step_protocol(lims, step_id=sample_artifact.parent_process.id)
        lims_library_prep = protocol_name.split("-", 1)[1].lower().strip()
        lims_library_prep = lims_library_prep.replace('illumina ', '')

    if not sample_meta['Prepped']:
        run_data['nr_samples_prepped'] += 1
        sample_meta['Prepped_date'] = sample_artifact.parent_process.date_run
        sample_meta['Prepped'] = True

    run_data['lims_library_prep'].add(lims_library_prep)
    run_data['libprep_date'].add(sample_artifact.parent_process.date_run)


def process_run_artifact(lims, sample_artifact, sample_meta: Dict[str, Any],
                        run_data: Dict[str, Any], run_meta: Dict[str, Any],
                        run_date, pid_sequenced: Dict[str, Set]) -> None:
    """Process run artifact and update metadata.

    Args:
        lims: LIMS connection
        sample_artifact: Artifact from run process
        sample_meta: Sample metadata dictionary
        run_data: Run data dictionary
        run_meta: Run metadata for API
        run_date: Run date
        pid_sequenced: Dictionary tracking when projects were sequenced
    """
    protocol_name = get_step_protocol(lims, step_id=sample_artifact.parent_process.id)
    run_data['lims_runtype'] = protocol_name.split("-", 1)[1].lower().strip()

    if not run_data['run_date']:
        run_data['run_date'] = run_date
        run_data['times_sequenced'] = 1
        run_meta['date'] = run_data['run_date']

    project_id = run_data['id']
    if project_id:
        pid_sequenced[project_id].add(sample_artifact.parent_process.date_run)

    if not sample_meta['Sequenced']:
        run_data['nr_samples_sequenced'] += 1
        sample_meta['Sequenced'] = True


def process_analysis_artifact(sample_artifact, sample, sample_meta: Dict[str, Any],
                             run_data: Dict[str, Any]) -> None:
    """Process analysis artifact and update metadata.

    Args:
        sample_artifact: Artifact from analysis process
        sample: Sample object
        sample_meta: Sample metadata dictionary
        run_data: Run data dictionary
    """
    run_data['analysis_date'].add(sample_artifact.parent_process.date_run)

    analysis_steps = ['Raw data (FastQ)']
    process_udf = sample_artifact.parent_process.udf

    # Add selected analysis steps
    analysis_options = [
        ('Mapping', 'Mapping'),
        ('Germline SNV/InDel calling', 'Germline SNV/InDel calling'),
        ('Read count analysis (mRNA)', 'Read count analysis (mRNA)'),
        ('Differential expression analysis + figures (mRNA)',
         'Differential expression analysis + figures (mRNA)'),
        ('CNV + SV calling', 'CNV + SV calling'),
        ('Somatic calling (tumor/normal pair)', 'Somatic calling (tumor/normal pair)')
        # "WGS : Mapping, Germline SNV/InDel, CNV & SV",
        # "RNA-seq : Mapping & Read count analysis (mRNA)"
        # "Single Cell Analysis"
    ]

    for udf_name, step_name in analysis_options:
        if process_udf.get(udf_name):
            analysis_steps.append(step_name)

    requested_analysis = "|".join(sorted(sample.udf['Analysis'].split(",")))
    lims_analysis = "|".join(sorted(analysis_steps))

    run_data['requested_analysis'].add(requested_analysis)
    sample_meta['Analysis'] = sample.udf['Analysis']
    run_data['lims_analysis'].add(lims_analysis)

    # if run_data['requested_analysis'] != run_data['lims_analysis']:
    #     run_data['errors'].add(
    #         f"Warning: Analysis type {lims_analysis} in LIMS doesn't match "
    #         f"analysis {requested_analysis}"
    #     )

    if not sample_meta['Analyzed']:
        run_data['nr_samples_analyzed'] += 1
        sample_meta['Analyzed_date'] = sample_artifact.parent_process.date_run
        sample_meta['Analyzed'] = True


def set_project_metadata(sample, pool, run_data: Dict[str, Any]) -> None:
    """Set project metadata from sample information (only once per project).

    Args:
        sample: Sample object
        pool: Pool artifact
        run_data: Run data dictionary to update
    """
    run_data['first_submission_date'] = sample.date_received

    if 'Sequencing Succesful' in pool.udf:
        run_data['succesful'] = pool.udf['Sequencing Succesful']

    run_data['name'] = sample.project.name
    run_data['id'] = sample.project.id
    run_data['open_date'] = sample.project.open_date

    # Handle project comments
    if 'Comments and agreements' in sample.project.udf:
        comments = sample.project.udf['Comments and agreements']
        run_data['project_comments'] = (
            comments.replace('\n', ' ').replace('\r', '').replace(';', ',')
        )

    # Contact information
    researcher = sample.project.researcher
    run_data['contact_name'] = f"{researcher.first_name} {researcher.last_name}"
    run_data['contact_email'] = researcher.email
    run_data['lab_name'] = researcher.lab.name

    # Budget information
    if 'Budget Number' in sample.udf:
        run_data['budget_nr'] = sample.udf['Budget Number']
    else:
        run_data['errors'].add("Warning: No Budgetnumber found")

    if 'Order Number' in sample.udf:
        run_data['order_nr'] = sample.udf['Order Number']

    # Billing address
    billing_addr = researcher.lab.billing_address
    run_data['institute'] = billing_addr['institution']
    run_data['postalcode'] = billing_addr['postalCode']
    run_data['city'] = billing_addr['city']
    run_data['country'] = billing_addr['country']
    run_data['department'] = billing_addr['department']
    run_data['street'] = billing_addr['street']

    # VAT and debtor numbers
    if 'UMCU_DebNr' in researcher.lab.udf:
        run_data['deb_nr'] = researcher.lab.udf['UMCU_DebNr']
    if 'UMCU_VATNr' in researcher.lab.udf:
        run_data['vat_nr'] = researcher.lab.udf['UMCU_VATNr']


def fetch_project_costs(project_id: str, run_meta: Dict[str, Any]) -> Dict[str, Any]:
    """Fetch project costs from the portal API.

    Args:
        project_id: Project ID
        run_meta: Run metadata for cost calculation

    Returns:
        Cost data from API or error information
    """
    url = f'{Config.PORTAL_URL}/finance/projectcosts/{project_id}'
    headers = {
        'Content-type': 'application/json; charset=utf-8',
        'Authorization': f'Bearer {Config.PORTAL_API_KEY}'
    }

    try:
        response = requests.post(
            url,
            headers=headers,
            data=json.dumps(run_meta, indent=4),
            timeout=30
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return {'error': f'Failed to retrieve costs - {str(e)}'}


def update_run_costs(run_data: Dict[str, Any], costs: Dict[str, Any],
                    application: str) -> None:
    """Update run data with cost information from API response.

    Args:
        run_data: Run data dictionary to update
        costs: Cost data from API
        application: Application name for cost lookup
    """
    if 'error' in costs:
        run_data['errors'].add(costs['error'])
        return

    try:
        run_data['isolation_step_costs'] = f"{float(costs['Isolation']['step_cost']):.2f}"
        run_data['isolation_personell_costs'] = f"{float(costs['Isolation']['personell_cost']):.2f}"
        run_data['libprep_step_costs'] = f"{float(costs['Library Prep']['step_cost']):.2f}"
        run_data['libprep_personell_costs'] = f"{float(costs['Library Prep']['personell_cost']):.2f}"
        run_data['run_step_costs'] = f"{float(costs[application]['step_cost']):.2f}"
        run_data['run_personell_costs'] = f"{float(costs[application]['personell_cost']):.2f}"
        run_data['analysis_step_costs'] = f"{float(costs['Analysis']['step_cost']):.2f}"
        run_data['analysis_personell_costs'] = f"{float(costs['Analysis']['personell_cost']):.2f}"
        run_data['total_step_costs'] = f"{float(costs['Total']['step_cost']):.2f}"
        run_data['total_personell_costs'] = f"{float(costs['Total']['personell_cost']):.2f}"
    except (KeyError, ValueError) as e:
        run_data['errors'].add(f"Warning: Error parsing cost data - {str(e)}")


def convert_sets_to_strings(data_dict: Dict[str, Any]) -> None:
    """Convert all set values in dictionary to comma-separated strings.

    Args:
        data_dict: Dictionary to process in-place
    """
    for key, value in data_dict.items():
        if isinstance(value, set):
            data_dict[key] = ",".join(str(item) for item in sorted(value))


def deduplicate_runs(runs: Dict[str, Dict[str, Dict[str, Any]]]) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Deduplicate runs based on project ID and run date.

    Args:
        runs: Dictionary of runs organized by pool and project

    Returns:
        Deduplicated runs dictionary
    """
    runs_dedup = {}
    unique_ids = []

    for pool_id in runs:
        for project_id in runs[pool_id]:
            unique_id = f"{project_id}-{runs[pool_id][project_id]['run_date']}"

            if unique_id not in unique_ids:
                unique_ids.append(unique_id)
                if pool_id not in runs_dedup:
                    runs_dedup[pool_id] = {}
                runs_dedup[pool_id][project_id] = runs[pool_id][project_id]
                convert_sets_to_strings(runs_dedup[pool_id][project_id])

    return runs_dedup


def get_seq_finance(lims, step_uri: str) -> str:
    """Calculate costs for all sequencing runs included in the step.

    Args:
        lims: LIMS connection
        step_uri: Step URI

    Returns:
        Rendered CSV template with finance data

    Raises:
        FinanceError: If finance calculation fails
    """
    step_details = Step(lims, uri=step_uri).details
    runs = {}
    pool_samples = {}
    pid_sequenced = {}

    for io_map in step_details.input_output_maps:
        if io_map[1]['output-generation-type'] == 'PerAllInputs':
            continue  # Skip the billing table file

        pool = io_map[0]['uri']
        pool_samples[pool.id] = {}
        runs[pool.id] = {}

        run_date = None
        pool_parent_process = pool.parent_process  # BCL to FastQ

        # Try to determine the date this run was sequenced
        if pool_parent_process:
            for parent_io_map in pool_parent_process.input_output_maps:
                if parent_io_map[1]['limsid'] == pool.id:
                    processes = lims.get_processes(inputartifactlimsid=parent_io_map[0]['limsid'])

                    if len(processes) == 1:  # Only when run step has been skipped in LIMS
                        run_date = parent_io_map[0]['uri'].parent_process.date_run

                    for process in processes:

                        if process.type.name in Config.RUN_PROCESSES:
                            run_date = process.date_run
                            break

        # Group samples by project
        for sample in pool.samples:
            project_id = sample.project.id
            if project_id not in pool_samples[pool.id]:
                pool_samples[pool.id][project_id] = []
            pool_samples[pool.id][project_id].append(sample)

        # Process each project in the pool
        for project_id in pool_samples[pool.id]:
            application = None
            platform = None

            run_meta = initialize_run_metadata()

            if project_id not in pid_sequenced:
                pid_sequenced[project_id] = set()

            if project_id not in runs[pool.id]:
                runs[pool.id][project_id] = initialize_project_run_data(pool.id)

            samples = lims.get_samples(projectlimsid=project_id)
            if len(pool_samples[pool.id][project_id]) != len(samples):
                # print(project_id, len(samples),len(pool_samples[pool.id][project_id]))
                runs[pool.id][project_id]['errors'].add(
                    'Warning: Number of samples sequenced not equal to number of samples submitted!'
                )

            # Process each sample
            for sample in samples:
                sample_meta = initialize_sample_metadata()

                runs[pool.id][project_id]['nr_samples_submitted'] += 1
                runs[pool.id][project_id]['received_date'].add(sample.date_received)
                runs[pool.id][project_id]['type'].add(sample.udf['Sample Type'])
                sample_meta['Sample Type'] = sample.udf['Sample Type']

                # Extract sample UDFs
                if 'Library prep kit' in sample.udf:
                    runs[pool.id][project_id]['requested_library_prep'].add(
                        sample.udf['Library prep kit']
                    )
                    run_meta['Library prep kit'] = sample.udf['Library prep kit']

                if 'Sequencing Coverage' in sample.udf:
                    sample_meta['Sequencing Coverage'] = sample.udf['Sequencing Coverage']

                runs[pool.id][project_id]['requested_runtype'].add(
                    sample.udf['Sequencing Runtype']
                )
                run_meta['Sequencing Runtype'] = sample.udf['Sequencing Runtype']

                # Handle NovaSeq X lanes
                if sample.udf['Platform'] == 'NovaSeq X':
                    if 'Number Lanes' in sample.udf:
                        runs[pool.id][project_id]['run_lanes'] = sample.udf['Number Lanes']
                        run_meta['Number Lanes'] = sample.udf['Number Lanes']
                    else:
                        runs[pool.id][project_id]['run_lanes'] = 8
                        run_meta['Number Lanes'] = 8
                        runs[pool.id][project_id]['errors'].add(
                            'Warning: No Number Lanes UDF found, using default of 8 (all) lanes!'
                        )

                # Handle GEM-X reads calculation
                elif 'GEM-X' in sample.udf['Sequencing Runtype']:
                    nr_cells = int(sample.udf.get('Number Cells', 0))
                    reads_cell = int(sample.udf.get('Reads Per Cell', 0))
                    reads = round((nr_cells * reads_cell) / 1e6)
                    sample_meta['Reads'] = reads
                    sample_meta['Add-ons'] = sample.udf.get('Add-ons', [])

                # Set platform and application
                if not platform:
                    platform = sample.udf['Platform']

                if not application:
                    application = sample.project.udf['Application']

                # Normalize application and platform names
                application, platform = normalize_application_platform(application, platform)

                runs[pool.id][project_id]['platform'] = platform
                run_meta['Platform'] = platform
                runs[pool.id][project_id]['application'] = application
                run_meta['Application'] = application

                # Handle flowcell-only samples
                if 'Flowcell only' in sample.udf['Sequencing Runtype']:
                    sample_meta['Sequenced'] = True
                    run_meta['samples'].append(sample_meta)
                    run_meta['date'] = sample.date_received
                    runs[pool.id][project_id]['run_date'] = run_meta['date']
                else:
                    # Process sample artifacts
                    sample_artifacts = lims.get_artifacts(samplelimsid=sample.id)
                    for sample_artifact in sample_artifacts:
                        if not sample_artifact.parent_process or \
                           not sample_artifact.parent_process.date_run:
                            continue

                        process_name = sample_artifact.parent_process.type.name

                        # Handle different process types
                        if process_name in Config.ISOLATION_PROCESSES:
                            process_isolation_artifact(
                                sample_artifact, sample_meta, runs[pool.id][project_id]
                            )

                        elif process_name in Config.LIBPREP_PROCESSES and \
                             sample.udf['Sequencing Runtype'] not in [
                                'WGS at HMF', 'WGS', 'WES (100X Coverage)', 'RNA-seq'
                             ]:
                            process_libprep_artifact(
                                lims, sample, sample_artifact, sample_meta,
                                runs[pool.id][project_id]
                            )

                        # elif process_name in Config.RUN_PROCESSES or \
                        #      process_name in Config.LOAD_PROCESSES:
                        elif process_name in Config.RUN_PROCESSES:
                            process_run_artifact(
                                lims, sample_artifact, sample_meta,
                                runs[pool.id][project_id], run_meta,
                                run_date, pid_sequenced
                            )

                        elif process_name in Config.ANALYSIS_PROCESSES and application != 'SNP Fingerprinting':
                            process_analysis_artifact(
                                sample_artifact, sample, sample_meta,
                                runs[pool.id][project_id]
                            )

                    if not pid_sequenced[project_id] and run_date:
                        run_meta['date'] = run_date
                        if not sample_meta['Sequenced']:
                            runs[pool.id][project_id]['nr_samples_sequenced'] +=1
                            sample_meta['Sequenced'] = True

                run_meta['samples'].append(sample_meta)

                # Set project metadata (only once)
                if not runs[pool.id][project_id]['name']:
                    set_project_metadata(sample, pool, runs[pool.id][project_id])

            # Calculate costs via API
            if len(pid_sequenced[project_id]) > 1:
                runs[pool.id][project_id]['errors'].add(
                    "Warning: Run was sequenced before more than once!"
                )
                # In case of reruns always bill at the costs of the oldest run
                run_meta['date'] = min(pid_sequenced[project_id])

            if not run_meta['date'] and pid_sequenced[project_id]:
                run_meta['date'] = min(pid_sequenced[project_id])
                runs[pool.id][project_id]['run_date'] = run_meta['date']
            # elif not run_meta['date'] and run_date:
            #     run_meta['date'] = run_date
            #     runs[pool.id][project_id]['run_date'] = run_meta['date']
                    # if not sample_meta['Sequenced']:
                    #     run_data['nr_samples_sequenced'] += 1
                    #     sample_meta['Sequenced'] = True
                ###Check if sample_meta['Sequenced'] = True needs to be set####

            # Fetch and update costs
            costs = fetch_project_costs(project_id, run_meta)
            update_run_costs(runs[pool.id][project_id], costs, application)

    # Deduplicate and convert sets to strings
    runs_dedup = deduplicate_runs(runs)
    return renderTemplate('seq_finance_overview_template.csv', {'pools': runs_dedup})


def get_snp_finance(lims, step_uri: str) -> str:
    """Calculate costs for SNP fingerprinting runs.

    Args:
        lims: LIMS connection
        step_uri: Step URI

    Returns:
        Rendered CSV template with finance data

    Raises:
        FinanceError: If finance calculation fails
    """
    step_details = Step(lims, uri=step_uri).details
    runs = {}

    # Get the input artifacts (which is a pool of samples)
    for io_map in step_details.input_output_maps:
        pool = io_map[0]['uri']

        for sample in pool.samples:
            try:
                budget_nr = sample.udf['Budget Number']
            except KeyError:
                raise FinanceError(f'No budgetnumber found for run {sample.project.id}')

            run_key = sample.project.id + budget_nr

            if run_key not in runs:
                researcher = sample.project.researcher
                runs[run_key] = {
                    'errors': set(),
                    'name': sample.project.name,
                    'id': sample.project.id,
                    'open_date': sample.project.open_date,
                    'samples': {},
                    'first_submission_date': None,
                    'received_date': None,
                    'description': set(),
                    'type': set(),
                    'isolation_step_costs': 0,
                    'isolation_personell_costs': 0,
                    'plate_step_costs': 0,
                    'plate_personell_costs': 0,
                    'total_step_costs': 0,
                    'total_personell_costs': 0,
                    'contact_name': f"{researcher.first_name} {researcher.last_name}",
                    'contact_email': researcher.email,
                    'lab_name': researcher.lab.name,
                    'budget_nr': budget_nr,
                    'institute': researcher.lab.billing_address['institution'],
                    'postalcode': researcher.lab.billing_address['postalCode'],
                    'city': researcher.lab.billing_address['city'],
                    'country': researcher.lab.billing_address['country'],
                    'department': researcher.lab.billing_address['department'],
                    'street': researcher.lab.billing_address['street']
                }

            sample_key = pool.id + sample.id
            if sample_key not in runs[run_key]['samples']:
                runs[run_key]['samples'][sample_key] = sample
                runs[run_key]['received_date'] = sample.date_received
                runs[run_key]['type'].add(sample.udf['Sample Type'])

                if 'Description' in sample.udf:
                    runs[run_key]['description'].add(sample.udf['Description'])

    # Calculate costs for each run
    for run_id in runs:
        run_meta = {
            'Application': 'SNP Fingerprinting',
            'Platform': '60 SNP NimaGen panel',
            'Sequencing Runtype': '60 SNP NimaGen panel',
            'Library prep kit': None,
            'Number Lanes': None,
            'date': runs[run_id]['received_date'],
            'samples': []
        }

        for sample_id in runs[run_id]['samples']:
            sample = runs[run_id]['samples'][sample_id]
            run_meta['samples'].append({
                'Sample Type': sample.udf['Sample Type'],
                'Analysis': '',
                'Sequencing Coverage': 0,
                'Isolated': 'unisolated' in sample.udf['Sample Type'].lower(),
                'Prepped': False,
                'Sequenced': False,
                'Analyzed': False,
            })

        # Fetch and update costs
        costs = fetch_project_costs(runs[run_id]["id"], run_meta)

        if 'error' in costs:
            runs[run_id]['errors'].add(costs['error'])
        else:
            try:
                runs[run_id]['isolation_step_costs'] = f"{float(costs['Isolation']['step_cost']):.2f}"
                runs[run_id]['isolation_personell_costs'] = f"{float(costs['Isolation']['personell_cost']):.2f}"
                runs[run_id]['plate_step_costs'] = f"{float(costs['SNP Fingerprinting']['step_cost']):.2f}"
                runs[run_id]['plate_personell_costs'] = f"{float(costs['SNP Fingerprinting']['personell_cost']):.2f}"
                runs[run_id]['total_step_costs'] = f"{float(costs['Total']['step_cost']):.2f}"
                runs[run_id]['total_personell_costs'] = f"{float(costs['Total']['personell_cost']):.2f}"
            except (KeyError, ValueError) as e:
                runs[run_id]['errors'].add(f"Warning: Error parsing cost data - {str(e)}")

    # Convert sets to strings
    for run_id in runs:
        convert_sets_to_strings(runs[run_id])

    return renderTemplate('snp_finance_overview_template.csv', {'runs': runs})


def run(lims, step_uri: str, output_file) -> None:
    """Run the finance overview generation based on protocol.

    Args:
        lims: LIMS connection
        step_uri: Step URI
        output_file: Output file handle

    Raises:
        FinanceError: If protocol is unknown or finance calculation fails
    """
    protocol_name = get_step_protocol(lims, step_uri=step_uri)

    if protocol_name.startswith("USEQ - Post Sequencing"):
        finance_table = get_seq_finance(lims, step_uri)
        output_file.write(finance_table)
    elif protocol_name.startswith("USEQ - Post Fingerprinting"):
        finance_table = get_snp_finance(lims, step_uri)
        output_file.write(finance_table)
    else:
        raise FinanceError(f"Unknown protocol: {protocol_name}")
