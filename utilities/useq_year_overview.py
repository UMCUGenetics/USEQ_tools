"""Module for generating yearly overview reports of LIMS projects."""

from config import Config
from genologics.lims import Lims
from typing import Dict, Any, Optional, TextIO
from collections import defaultdict
from tqdm import tqdm

import csv
def get_year_overview(lims: Lims, year: Optional[str]) -> Dict[str, Dict[str, Any]]:
    """Generate overview of projects by year, platform, run type, and sample type.

    Args:
        lims: LIMS instance
        year: The four-digit year (e.g., '2025') to filter projects based on their `close_date`. If None, projects from all years are processed.

    Returns:
        Nested dictionary with structure:
        {year: {platform: {run_types: {run_type: {sample_types: {sample_type: {runs, samples}}}}}}}

    Note:
        - Projects without a `close_date` or a billing process are ignored.
        - Projects without a platform but labeled as 'SNP' are automatically
          categorized under 'SNP Fingerprinting'.
    """
    # Define a recursive factory for the nested structure
    # Structure: Year -> Platform -> "run_types" -> RunType -> "sample_types" -> SampleType -> Metrics
    tree = lambda: defaultdict(lambda: {
        'run_types': defaultdict(lambda: {
            'sample_types': defaultdict(lambda: {'runs': 0, 'samples': 0})
        })
    })

    overview = defaultdict(tree)

    all_projects = lims.get_projects()

    for project in tqdm(all_projects, desc="Processing Projects", unit="proj"):
        # 1. Validation and Filtering
        if not project.close_date:
            continue

        if year and not project.close_date.startswith(year):
            continue

        if 'Application' not in project.udf:
            continue

        if project.udf['Application'] not in Config.PROJECT_TYPES.values():
            continue

        # 2. Sample and Process Retrieval
        samples = lims.get_samples(projectlimsid=project.id)
        if not samples:
            continue

        project_processes = lims.get_processes(
            type=['USEQ - Ready for billing', 'Ready for billing'],
            projectname=project.name
        )
        if not project_processes:
            continue

        # 3. Data Extraction
        billing_year = project.close_date.split("-")[0]
        sample_type = samples[0].udf.get('Sample Type')
        platform = samples[0].udf.get('Platform')
        run_type = samples[0].udf.get('Sequencing Runtype')
        application = project.udf['Application']

        # Handle missing platform for SNP projects
        if not platform and 'SNP' in application:
            platform = 'SNP Fingerprinting'
        elif not platform:
            print(f"Warning: Project {project.id} has no platform defined")
            continue

        # 4. Increment Counters (No "if" checks needed due to defaultdict)
        stats = overview[billing_year][platform]['run_types'][run_type]['sample_types'][sample_type]
        stats['runs'] += 1
        stats['samples'] += len(samples)

    # Convert back to standard dict for cleaner output/serialization if needed
    return overview


def print_overview(overview: Dict[str, Dict[str, Any]], overview_file: TextIO):
    """Write overview data to CSV file.

    Args:
        overview: Nested dictionary from get_year_overview()
        overview_file: File object to write CSV data to
    """
    # Write header
    # Initialize the CSV writer with semicolon delimiter
    writer = csv.writer(overview_file, delimiter=';', lineterminator='\n')

    # Write header
    writer.writerow(["Year", "Platform", "Run type", "Sample Type", "Runs", "Samples"])

    # Flatten and write data rows
    for year in sorted(overview.keys()):
        for platform in sorted(overview[year].keys()):
            run_types_dict = overview[year][platform].get('run_types', {})

            for run_type in sorted(run_types_dict.keys()):
                sample_types_dict = run_types_dict[run_type].get('sample_types', {})

                for sample_type in sorted(sample_types_dict.keys()):
                    stats = sample_types_dict[sample_type]

                    # Prepare row data with 'N/A' fallback for missing keys
                    row = [
                        year or 'N/A',
                        platform or 'N/A',
                        run_type or 'N/A',
                        sample_type or 'N/A',
                        stats.get('runs', 0),
                        stats.get('samples', 0)
                    ]
                    writer.writerow(row)


def run(lims: Lims, year: str, overview_file: TextIO):
    """Generate and write yearly overview report.

    Args:
        lims: LIMS instance
        year: The four-digit year (e.g., '2025') to filter projects based on their `close_date`. If None, projects from all years are processed.
        overview_file: File object to write CSV report to
    """
    overview = get_year_overview(lims, year)
    print_overview(overview, overview_file)
