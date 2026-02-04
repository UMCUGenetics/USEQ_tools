"""Module for handling the demultiplexing, transfer to nextcloud and archiving of Illumina sequencing runs."""

import logging
import subprocess
import xml.dom.minidom
import shlex
import sys
import traceback
import csv
import json
import os
import time
import shutil
import re
from genologics.entities import Project
from genologics.lims import Lims
from itertools import islice
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set, Any, Union
from modules.useq_illumina_parsers import get_expected_reads, parse_sample_sheet
from modules.useq_nextcloud import NextcloudUtil
from modules.useq_template import TEMPLATE_PATH, TEMPLATE_ENVIRONMENT, render_template
from modules.useq_mail import send_mail
from config import Config


class RunManagerError(Exception):
    """Custom exception for Run Manager operations."""
    pass


class DemultiplexingError(RunManagerError):
    """Exception for demultiplexing-related errors."""
    pass


class TransferError(RunManagerError):
    """Exception for transfer-related errors."""
    pass


def add_flowcell_to_fastq(project_directory: Path, flowcell_id: str, logger: logging.Logger):
    """
    Add flowcell id to fastq.gz filename.

    Args:
        project_directory (Path): Path to project directory containing FASTQ files
        flowcell_id (str): Flowcell identifier to add to filenames
        logger (logging.Logger): Logger instance
    """
    logger.info('Adding flowcell to fastqs')

    for fastq in project_directory.rglob("*.fastq.gz"):
        filename_parts = fastq.name.split('_')
        if filename_parts[1] != flowcell_id:
            filename_parts.insert(1, flowcell_id)
            new_filename = '_'.join(filename_parts)
            fastq.rename(fastq.parent / new_filename)


def cleanup_fastq_files(run_dir: Path, logger: logging.Logger):
    """
    Delete FastQ files from run directory.

    Args:
        run_dir (Path): Path to run directory
        logger (logging.Logger): Logger instance
    """
    logger.info('Deleting FastQ files')
    for file in run_dir.rglob("*.gz"):
        if file.suffix in ['.fastq.gz', '.fq.gz'] or file.name.endswith(('.fastq.gz', '.fq.gz')):
            try:
                file.unlink()
            except OSError as e:
                logger.warning(f"Could not delete {file}: {e}")


def reverse_complement(seq: str) -> str:
    """
    Calculate reverse complement of DNA sequence.

    Args:
        seq (str): DNA sequence string

    Returns:
        Reverse complement sequence
    """
    complement_map = {'A': 'T', 'C': 'G', 'G': 'C', 'T': 'A'}
    return ''.join(complement_map.get(base, base) for base in reversed(seq))


def process_sample_indices(header: List[str], sample: List[str]) -> Tuple[List[str], List[str]]:
    """
    Process and clean sample indices, creating forward and reverse complement versions.

    Args:
        header (List[str]): Sample sheet header
        sample (List[str]): Sample data row

    Returns:
        Tuple of (forward_sample, reverse_sample)
    """
    sample = sample.copy()

    # Process index
    index_col = header.index('index')
    if sample[index_col].count('N') == len(sample[index_col]):
        sample[index_col] = sample[index_col].replace("N", "A")
    else:
        sample[index_col] = sample[index_col].replace("N", "")

    sample_rev = sample.copy()

    # Process index2 if present
    if 'index2' in header:
        index2_col = header.index('index2')
        if sample[index2_col].count('N') == len(sample[index2_col]):
            sample[index2_col] = sample[index2_col].replace("N", "A")
            sample_rev[index2_col] = sample_rev[index2_col].replace("N", "A")

        sample_rev[index2_col] = reverse_complement(sample_rev[index2_col])
    else:
        sample_rev[index_col] = reverse_complement(sample_rev[index_col])

    return sample, sample_rev


def run_system_command(command: str, logger: logging.Logger, shell: bool = False) -> bool:
    """
    Execute system command with proper error handling.

    Args:
        command (str): Command to execute
        logger (logging.Logger): Logger instance
        shell (bool): Whether to use shell execution

    Returns:
        True if command succeeded, False otherwise
    """
    logger.info(f'Running command: {command}')

    try:
        if shell:
            result = subprocess.run(command, check=True, shell=True,
                                  stderr=subprocess.PIPE, text=True)
        else:
            command_pieces = shlex.split(command)
            result = subprocess.run(command_pieces, check=True,
                                  stderr=subprocess.PIPE, text=True)
        return True

    except FileNotFoundError as e:
        logger.error(f'Process failed - executable not found: {e}')
        return False

    except subprocess.CalledProcessError as e:
        logger.error(f'Process failed with return code {e.returncode}: {e}')
        if e.stderr:
            logger.error(f'Error output: {e.stderr}')
        return False


def update_status(status_file: Path, status: Dict[str, Any]) -> None:
    """Update status file with current processing status.

    Args:
        status_file (Path): Path to status file
        status (Dict[str, Any]): Status dictionary to write
    """
    with open(status_file, 'w') as f:
        json.dump(status, f, indent=4)


def validate_demultiplexing_stats(stats: Dict[str, Any], pid: str, project_data: Dict[str, Any],
                                run_data: Dict[str, Any], logger: logging.Logger) -> bool:
    """
    Validate demultiplexing statistics to determine if demux was successful.

    Args:
        stats (Dict[str, Any]): Demultiplexing statistics
        pid (str): Project ID
        project_data (Dict[str, Any]): Project information
        run_data (Dict[str, Any]): Run information
        logger (logging.Logger): Logger instance

    Returns:
        True if demultiplexing passed validation, False otherwise
    """
    undetermined_ratio = stats['undetermined_reads'] / stats['total_reads']

    if undetermined_ratio < 0.40:
        logger.info(f'Demultiplexing check stats PASSED (undetermined ratio: {undetermined_ratio:.3f})')
        return True

    # Additional validation for high undetermined ratios
    for lane in project_data['on_lanes']:
        if lane not in stats['total_reads_lane'] or lane not in run_data['lanes']:
            continue

        total_reads_lane = stats['total_reads_lane'][lane]
        nr_samples_lane = len(run_data['lanes'][lane]['samples'])
        nr_samples_project = len(project_data['sample_ids'])

        if nr_samples_lane == 0:
            continue

        avg_reads_per_sample = total_reads_lane / nr_samples_lane
        expected_project_reads = avg_reads_per_sample * nr_samples_project

        if pid in stats['total_reads_project']:
            avg_reads_project_lane = stats['total_reads_project'][pid] / len(project_data['on_lanes'])

            if expected_project_reads + avg_reads_project_lane > 0:
                perc_diff = abs(expected_project_reads - avg_reads_project_lane) / \
                           ((expected_project_reads + avg_reads_project_lane) / 2)

                logger.info(f'Lane {lane} validation - Expected: {expected_project_reads:.0f}, '
                           f'Actual: {avg_reads_project_lane:.0f}, Diff: {perc_diff:.3f}')

                if perc_diff > 1.5:
                    logger.warning(f'Demultiplexing check FAILED for lane {lane} (diff: {perc_diff:.3f})')
                    return False

    logger.info('Demultiplexing check stats PASSED after detailed validation')
    return True


def check_demultiplexing_samplesheet(sample_sheet: Path, pid: str, project_data: Dict[str, Any], run_data: Dict[str, Any], first_tile: str, logger: logging.Logger) -> bool:
    """
    Test demultiplexing with given samplesheet.

    Args:
        sample_sheet: (Path) Path to sample sheet
        pid (str): Project ID
        project_data (Dict[str, Any]): Project information
        run_data (Dict[str, Any]): Run information
        first_tile (str): First tile for testing
        logger (logging.Logger): Logger instance

    Returns:
        True if demultiplexing test passed, False otherwise
    """
    run_dir = "/" + "/".join(sample_sheet.parts[1:sample_sheet.parts.index('Conversion')])
    demux_out_dir = sample_sheet.parent / sample_sheet.stem

    logger.info(f'Running demultiplexing check on {sample_sheet.name}')

    command = (f'{Config.CONV_BCLCONVERT}/bcl-convert --bcl-input-directory {run_dir} '
              f'--output-directory {demux_out_dir} --sample-sheet {sample_sheet} '
              f'--bcl-sampleproject-subdirectories true --force --tiles {first_tile}')

    if not run_system_command(command, logger):
        logger.error(f'Failed to run demultiplexing check on {sample_sheet.name}')
        raise DemultiplexingError(f'Demultiplexing check failed for {sample_sheet.name}')

    logger.info(f'Checking demultiplexing stats for {demux_out_dir}/Reports')
    stats = parse_conversion_stats(demux_out_dir / 'Reports')

    return validate_demultiplexing_stats(stats, pid, project_data, run_data, logger)


def demultiplex_project(run_dir: Path, pid: str, project_data: Dict[str, Any], run_data: Dict[str, Any], master_sheet: Dict[str, Any], first_tile: str, logger: logging.Logger) -> bool:
    """
    Demultiplex samples for a specific project.

    Args:
        run_dir (Path): Run directory path
        pid (str): Project ID
        project_data (Dict[str, Any]): Project information
        run_data (Dict[str, Any]): Run information
        master_sheet (Dict[str, Any]): Master sample sheet data
        first_tile (str): First tile for testing
        logger (logging.Logger): Logger instance

    Returns:
        True if demultiplexing succeeded, False otherwise
    """
    samples = project_data['samples']
    project_directory = run_dir / 'Conversion' / pid
    project_directory.mkdir(parents=True, exist_ok=True)

    demux_directory = project_directory / 'Demux-check'
    demux_directory.mkdir(parents=True, exist_ok=True)

    flowcell = run_dir.name.split("_")[-1]
    logger.info('Moving on with demux test.')

    # Create both forward and reverse sample sheets
    sample_sheet = demux_directory / f'SampleSheet-{pid}.csv'
    sample_sheet_rev = demux_directory / f'SampleSheet-{pid}-rev.csv'

    with open(sample_sheet, 'w') as ss, open(sample_sheet_rev, 'w') as ssr:
        logger.info(f'Creating samplesheets {sample_sheet} and {sample_sheet_rev}')

        # Write headers
        ss.write(master_sheet['top'])
        ss.write(f'{",".join(master_sheet["header"])}\n')
        ssr.write(master_sheet['top'])
        ssr.write(f'{",".join(master_sheet["header"])}\n')

        # Process samples
        for sample in samples:
            forward_sample, reverse_sample = process_sample_indices(master_sheet['header'], sample)
            ss.write(f'{",".join(forward_sample)}\n')
            ssr.write(f'{",".join(reverse_sample)}\n')

    # Test both sample sheets to find the correct one
    correct_samplesheet = None
    try:
        if check_demultiplexing_samplesheet(sample_sheet, pid, project_data, run_data, first_tile, logger):
            correct_samplesheet = sample_sheet
        elif check_demultiplexing_samplesheet(sample_sheet_rev, pid, project_data, run_data, first_tile, logger):
            correct_samplesheet = sample_sheet_rev
    except DemultiplexingError:
        logger.error(f'Could not create a correct samplesheet for projectID {pid}, skipping demux.')
        return False

    if not correct_samplesheet:
        logger.error(f'Could not create a correct samplesheet for projectID {pid}, skipping demux.')
        return False

    # Move correct samplesheet and run full demultiplexing
    final_samplesheet = project_directory / correct_samplesheet.name
    shutil.move(str(correct_samplesheet), str(final_samplesheet))

    logger.info('Starting demultiplexing')
    command = (f'{Config.CONV_BCLCONVERT}/bcl-convert --bcl-input-directory {run_dir} '
              f'--output-directory {project_directory} --force --sample-sheet {final_samplesheet}')

    if Config.DEVMODE:
        command += f" --tiles {first_tile} "

    if not run_system_command(command, logger):
        logger.error(f'Failed to run demultiplexing for projectID {pid}.')
        raise DemultiplexingError(f'Demultiplexing failed for project {pid}')

    add_flowcell_to_fastq(project_directory, flowcell, logger)
    return True


def filter_stats_by_samples(lims: Lims, pid: str, pid_staging: Path, report_dir: Path):
    """
    Filter adapter and demultiplexing stats to include only project samples.

    Args:
        lims (Lims): LIMS connection
        pid (str): Project ID
        pid_staging (Path): Staging directory path
        report_dir (Path): Reports directory path
    """
    samples = lims.get_samples(projectlimsid=pid)
    sample_names = [x.name for x in samples]

    # Filter adapter metrics
    adapter_metrics = report_dir / 'Adapter_Metrics.csv'
    adapter_metrics_filtered = pid_staging / 'Adapter_Metrics.csv'

    if adapter_metrics.is_file():
        with open(adapter_metrics, 'r') as original, open(adapter_metrics_filtered, 'w') as filtered:
            for line in original:
                parts = line.split(',')
                if line.startswith('Lane') or (len(parts) > 1 and parts[1] in sample_names):
                    filtered.write(line)

    # Filter demultiplexing stats
    demultiplex_stats = report_dir / 'Demultiplex_Stats.csv'
    demultiplex_stats_filtered = pid_staging / 'Demultiplex_Stats.csv'

    if demultiplex_stats.is_file():
        with open(demultiplex_stats, 'r') as original, open(demultiplex_stats_filtered, 'w') as filtered:
            for line in original:
                parts = line.split(',')
                if line.startswith('Lane') or (len(parts) > 1 and parts[1] in sample_names):
                    filtered.write(line)


def generate_run_statistics(lims: Lims, run_dir: Path, logger: logging.Logger) -> bool:
    """
    Generate comprehensive run statistics and quality reports.

    Args:
        lims (Lims): LIMS connection
        run_dir (Path): Run directory path
        logger (logging.Logger): Logger instance

    Returns:
        True if statistics generation succeeded
    """
    conversion_dir = run_dir / 'Conversion'
    stats_dir = run_dir / 'Conversion' / 'Reports'
    fastqc_dir = stats_dir / 'fastqc'
    multiqc_dir = stats_dir / 'multiqc'

    fastqc_dir.mkdir(parents=True, exist_ok=True)
    os.chdir(stats_dir)

    try:
        # Generate run summary
        logger.info('Generating run summary')
        with open(stats_dir / f'{run_dir.name}_summary.csv', 'w') as summary_csv:
            subprocess.run([f'{Config.CONV_INTEROP}/bin/summary', str(run_dir)],
                         stdout=summary_csv, check=True, stderr=subprocess.PIPE)

        # Generate plots
        plot_commands = [
            ('Intensity', 'Intensity'),
            ('BasePercent', 'BasePercent'),
        ]

        for metric_name, _ in plot_commands:
            logger.info(f'Generating {metric_name} plot')
            p1 = subprocess.Popen([f'{Config.CONV_INTEROP}/bin/plot_by_cycle', str(run_dir),
                                 f'--metric-name={metric_name}'], stdout=subprocess.PIPE)
            subprocess.run(['gnuplot'], stdin=p1.stdout, check=True, stderr=subprocess.PIPE)

        # Additional plots
        plot_types = [
            ('plot_by_lane', ['--metric-name=Clusters']),
            ('plot_flowcell', []),
            ('plot_qscore_heatmap', []),
            ('plot_qscore_histogram', [])
        ]

        for plot_type, args in plot_types:
            logger.info(f'Generating {plot_type}')
            cmd = [f'{Config.CONV_INTEROP}/bin/{plot_type}', str(run_dir)] + args
            p1 = subprocess.Popen(cmd, stdout=subprocess.PIPE)
            subprocess.run(['gnuplot'], stdin=p1.stdout, check=True, stderr=subprocess.PIPE)

        # Run FastQC and MultiQC if not in dev mode
        if not Config.DEVMODE:
            logger.info('Running FastQC')
            fastqc_command = (f'{Config.CONV_FASTQC}/fastqc -t 24 -q '
                            f'{run_dir}/Conversion/**/*_R*fastq.gz -o {fastqc_dir}')

            if not run_system_command(fastqc_command, logger, shell=True):
                logger.error('Failed to run FastQC.')
                raise subprocess.CalledProcessError(1, 'fastqc')

            logger.info('Running MultiQC')
            multiqc_command = (f'multiqc {fastqc_dir} -o {multiqc_dir} '
                             f'-n {run_dir.name}_multiqc_report.html')

            if not run_system_command(multiqc_command, logger, shell=True):
                logger.error('Failed to run MultiQC.')
                raise subprocess.CalledProcessError(1, 'multiqc')

    except subprocess.CalledProcessError as e:
        logger.error(f'Failed to generate run stats: {e}')
        raise

    # Consolidate statistics files
    consolidate_statistics_files(conversion_dir, stats_dir)

    # Upload to HPC
    upload_to_hpc(lims, run_dir, None, logger)
    return True


def consolidate_statistics_files(conversion_dir: Path, stats_dir: Path):
    """
    Consolidate statistics files from individual projects into combined files.

    Args:
        conversion_dir (Path): Conversion directory path
        stats_dir (Path): Statistics directory path
    """
    # Consolidate demultiplexing stats
    with open(stats_dir / 'Demultiplex_Stats.csv', 'w') as all_demux_stats:
        first_line = None
        for demultiplex_stats in conversion_dir.glob('*/Reports/Demultiplex_Stats.csv'):
            project_id = demultiplex_stats.parts[-3]
            with open(demultiplex_stats, 'r') as ds:
                if not first_line:
                    first_line = ds.readline().split(",")
                    first_line.insert(2, 'Sample_Project')
                    all_demux_stats.write(",".join(first_line))
                else:
                    ds.readline()  # Skip header

                for line in ds:
                    line_parts = line.split(",")
                    line_parts.insert(2, project_id)
                    all_demux_stats.write(",".join(line_parts))

    # Consolidate quality metrics
    with open(stats_dir / 'Quality_Metrics.csv', 'w') as all_qm_stats:
        first_line = None
        for qm_stats in conversion_dir.glob('*/Reports/Quality_Metrics.csv'):
            project_id = qm_stats.parts[-3]
            with open(qm_stats, 'r') as qs:
                if not first_line:
                    first_line = qs.readline().split(",")
                    first_line.insert(2, 'Sample_Project')
                    all_qm_stats.write(",".join(first_line))
                else:
                    qs.readline()  # Skip header

                for line in qs:
                    line_parts = line.split(",")
                    line_parts.insert(2, project_id)
                    all_qm_stats.write(",".join(line_parts))

    # Consolidate top unknown barcodes
    with open(stats_dir / 'Top_Unknown_Barcodes.csv', 'w') as all_top_unknown:
        first_line = None
        for tu_stats in conversion_dir.glob('*/Reports/Top_Unknown_Barcodes.csv'):
            with open(tu_stats, 'r') as tu:
                if not first_line:
                    first_line = tu.readline()
                    all_top_unknown.write(first_line)
                else:
                    tu.readline()  # Skip header

                all_top_unknown.write(tu.read())


def get_expected_yield(run_info_xml: Path, expected_reads: int) -> Dict[str, float]:
    """
    Calculate expected yield from run info.

    Args:
        run_info_xml (Path): Path to RunInfo.xml
        expected_reads (int): Expected number of reads

    Returns:
        Dictionary with expected yields for R1 and R2
    """
    run_info = xml.dom.minidom.parse(str(run_info_xml))
    yields = {'r1': 0, 'r2': 0}

    for read in run_info.getElementsByTagName('Read'):
        if read.getAttribute('IsIndexedRead') == 'N':
            read_number = int(read.getAttribute('Number'))
            num_cycles = float(read.getAttribute('NumCycles'))

            if read_number == 1:
                yields['r1'] = (num_cycles * expected_reads) / 1000000000
            else:
                yields['r2'] = (num_cycles * expected_reads) / 1000000000

    return yields


def parse_conversion_stats(reports_dir: Path) -> Dict[str, Any]:
    """
    Parse conversion statistics from reports directory.

    Args:
        reports_dir (Path): Path to reports directory

    Returns:
        Dictionary containing parsed statistics
    """
    demux_stats = reports_dir / 'Demultiplex_Stats.csv'
    qual_metrics = reports_dir / 'Quality_Metrics.csv'
    top_unknown = reports_dir / 'Top_Unknown_Barcodes.csv'

    stats = {
        'total_reads': 0,
        'total_reads_lane': {},
        'total_reads_project': {},
        'total_reads_lane_project': {},
        'undetermined_reads': 0,
        'samples': {},
        'top_unknown': {}
    }

    samples_tmp = {}

    # Parse demultiplexing stats
    if demux_stats.is_file():
        with open(demux_stats, 'r') as d:
            csv_reader = csv.DictReader(d)
            for row in csv_reader:
                sample_id = row['SampleID']
                lane = int(row['Lane'])
                project_id = row.get('Sample_Project', 'Unknown')

                # Initialize data structures
                if lane not in samples_tmp:
                    samples_tmp[lane] = {}
                if sample_id not in samples_tmp[lane]:
                    samples_tmp[lane][sample_id] = {
                        'ProjectID': project_id,
                        'Index': None,
                        '# Reads': 0,
                        '# Perfect Index Reads': 0,
                        '# One Mismatch Index Reads': 0,
                    }

                if project_id not in stats['total_reads_project']:
                    stats['total_reads_project'][project_id] = 0

                if lane not in stats['total_reads_lane_project']:
                    stats['total_reads_lane_project'][lane] = {}
                if project_id not in stats['total_reads_lane_project'][lane]:
                    stats['total_reads_lane_project'][lane][project_id] = 0

                reads = float(row['# Reads'])

                if sample_id == 'Undetermined':
                    stats['undetermined_reads'] += reads
                else:
                    stats['total_reads_project'][project_id] += reads
                    stats['total_reads_lane_project'][lane][project_id] += reads

                stats['total_reads'] += reads

                if lane not in stats['total_reads_lane']:
                    stats['total_reads_lane'][lane] = 0
                stats['total_reads_lane'][lane] += reads

                # Update sample data
                samples_tmp[lane][sample_id]['Index'] = row['Index']
                samples_tmp[lane][sample_id]['# Reads'] += int(reads)
                samples_tmp[lane][sample_id]['# Perfect Index Reads'] += int(row['# Perfect Index Reads'])
                samples_tmp[lane][sample_id]['# One Mismatch Index Reads'] += int(row['# One Mismatch Index Reads'])

    # Parse quality metrics if available
    if qual_metrics.is_file():
        with open(qual_metrics, 'r') as q:
            csv_reader = csv.DictReader(q)
            for row in csv_reader:
                sample_id = row['SampleID']
                lane = int(row['Lane'])
                read_num = row['ReadNumber']

                if lane in samples_tmp and sample_id in samples_tmp[lane]:
                    mqs_key = f'Read {read_num} Mean Quality Score (PF)'
                    q30_key = f'Read {read_num} % Q30'

                    if mqs_key not in samples_tmp[lane][sample_id]:
                        samples_tmp[lane][sample_id][mqs_key] = 0
                    if q30_key not in samples_tmp[lane][sample_id]:
                        samples_tmp[lane][sample_id][q30_key] = 0

                    samples_tmp[lane][sample_id][mqs_key] += float(row['Mean Quality Score (PF)'])
                    samples_tmp[lane][sample_id][q30_key] += float(row['% Q30'])

    # Process final sample statistics
    for lane, lane_samples in samples_tmp.items():
        if lane not in stats['samples']:
            stats['samples'][lane] = {}

        for sample_id, sample_data in lane_samples.items():
            stats['samples'][lane][sample_id] = {
                'SampleID': sample_id,
                'ProjectID': sample_data['ProjectID'],
                'Index': sample_data['Index'],
                '# Reads': sample_data['# Reads'],
                '# Perfect Index Reads': sample_data['# Perfect Index Reads'],
                '# One Mismatch Index Reads': sample_data['# One Mismatch Index Reads']
            }

            # Add quality metrics
            for read_number in ['1', '2', 'I1', 'I2']:
                mqs_key = f'Read {read_number} Mean Quality Score (PF)'
                q30_key = f'Read {read_number} % Q30'

                if mqs_key in sample_data:
                    stats['samples'][lane][sample_id][mqs_key] = sample_data[mqs_key]
                if q30_key in sample_data:
                    stats['samples'][lane][sample_id][q30_key] = sample_data[q30_key] * 100

    # Parse top unknown barcodes
    if top_unknown.is_file():
        with open(top_unknown, 'r') as t:
            csv_reader = csv.DictReader(t)
            for row in csv_reader:
                lane = int(row['Lane'])
                if lane not in stats['top_unknown']:
                    stats['top_unknown'][lane] = []
                if len(stats['top_unknown'][lane]) < 5:
                    stats['top_unknown'][lane].append(row)

    return stats


def parse_summary_stats(summary_file: Path) -> Dict[str, Any]:
    """
    Parse summary statistics from summary file.

    Args:
        summary_file (Path): Path to summary file

    Returns:
        Dictionary containing parsed summary statistics
    """
    stats = {'summary': [], 'all': {}}
    tmp = {}

    if not summary_file.is_file():
        return stats

    with open(summary_file, 'r') as sumcsv:
        lines = sumcsv.readlines()
        line_nr = 0

        while line_nr < len(lines):
            line = lines[line_nr].rstrip()
            if not line:
                line_nr += 1
                continue

            if line.startswith('Level'):
                header = [x.rstrip() for x in line.split(",")]
                line_nr += 1

                while line_nr < len(lines):
                    sub_line = lines[line_nr].rstrip()
                    if not sub_line:
                        break

                    cols = [x.rstrip() for x in sub_line.split(",")]
                    stats['summary'].append(dict(zip(header, cols)))
                    line_nr += 1

                    if sub_line.startswith('Total'):
                        break

            elif line.startswith('Read'):
                read = line.rstrip()
                line_nr += 1

                if line_nr < len(lines):
                    header_line = lines[line_nr].rstrip()
                    header = [x.rstrip() for x in header_line.split(",")]
                    line_nr += 1

                    if read not in tmp:
                        tmp[read] = {}

                    while line_nr < len(lines):
                        sub_line = lines[line_nr].rstrip()
                        if sub_line.startswith('Read') or sub_line.startswith('Extracted'):
                            break

                        cols = [x.rstrip().split(" ")[0] for x in sub_line.split(",")]
                        if len(cols) > 1 and cols[1] == "-":  # lane summary line
                            col_dict = dict(zip(header, cols))
                            tmp[read][col_dict['Lane']] = col_dict

                        line_nr += 1
            else:
                line_nr += 1

    # Process temporary data into final format
    for read in tmp:
        if '(I)' in read:  # Skip index reads
            continue

        for lane in tmp[read]:
            if lane not in stats['all']:
                stats['all'][lane] = {}
            stats['all'][lane][read] = tmp[read][lane]

    return stats


def upload_to_archive(run_dir: Path, logger: logging.Logger) -> bool:
    """
    Upload run directory to archive storage.

    Args:
        run_dir (Path): Run directory path
        logger (logging.Logger): Logger instance

    Returns:
        True if upload succeeded
    """
    # Determine machine name
    if 'MyRun' in run_dir.parents[0].name:
        machine = run_dir.parents[1].name
    else:
        machine = run_dir.parents[0].name

    command = "rsync -rahm "
    if Config.DEVMODE:
        command += "--dry-run "

    logger.info('Uploading run folder to archive storage')

    if machine == 'WES-WGS':
        command += f"--exclude '*jpg' {run_dir} {Config.USEQ_USER}@{Config.HPC_TRANSFER_SERVER}:{Config.HPC_ARCHIVE_DIR}/{machine}"
    else:
        command += f"--exclude '*jpg' --exclude '*fastq.gz' --exclude '*fq.gz' {run_dir} {Config.USEQ_USER}@{Config.HPC_TRANSFER_SERVER}:{Config.HPC_ARCHIVE_DIR}/{machine}"

    if not run_system_command(command, logger):
        logger.error('Failed upload run folder to archive storage')
        raise TransferError('Archive upload failed')

    return True


def upload_to_hpc(lims: Lims, run_dir: Path, pid: Optional[str], logger: logging.Logger) -> bool:
    """
    Upload run data to HPC storage.

    Args:
        lims (Lims): LIMS connection
        run_dir (Path): Run directory path
        pid (Optional[str]): Project ID (optional)
        logger (logging.Logger): Logger instance

    Returns:
        True if upload succeeded
    """
    # Determine machine name
    if 'MyRun' in run_dir.parents[0].name:
        machine = run_dir.parents[1].name
    else:
        machine = run_dir.parents[0].name

    command = '/usr/bin/rsync -rah --update --stats --verbose --prune-empty-dirs '
    if Config.DEVMODE:
        command += '--dry-run '

    if pid:
        project = Project(lims, id=pid)
        samples = lims.get_samples(projectlimsid=project.id)

        if samples:
            analysis_steps = samples[0].udf.get('Analysis', '').split(',')
            if len(analysis_steps) > 1 or project.udf.get('Application', '') == 'SNP Fingerprinting':
                command += '--exclude "*Undetermined_*.fastq.gz" '
                command += f'--include "Conversion/{pid}/*.fastq.gz" '
            else:
                command += f'--exclude "Conversion/{pid}/*.fastq.gz" '

        command += (f" --include '*/' --include 'md5sum.txt' --include 'SampleSheet.csv' "
                   f"--include 'RunInfo.xml' --include '*unParameters.xml' --include 'InterOp/**' "
                   f"--include '*/Conversion/{pid}/Reports/**' --include 'Data/Intensities/BaseCalls/Stats/**' "
                   f"--include '*.[pP][eE][dD]'")
    else:
        command += ' --exclude "*.fastq.gz" '
        command += (f" --include '*/' --include 'md5sum.txt' --include 'SampleSheet.csv' "
                   f"--include 'RunInfo.xml' --include '*unParameters.xml' --include 'InterOp/**' "
                   f"--include '*/Conversion/Reports/**' --include 'Data/Intensities/BaseCalls/Stats/**' "
                   f"--include '*.[pP][eE][dD]'")

    command += " --exclude '*' "
    command += f" {run_dir} {Config.USEQ_USER}@{Config.HPC_TRANSFER_SERVER}:/{Config.HPC_RAW_ROOT}/{machine}"

    logger.info('Uploading run folder to HPC')

    if not run_system_command(command, logger):
        logger.error('Failed upload run folder to HPC')
        raise TransferError('HPC upload failed')

    return True


def upload_to_nextcloud(lims: Lims, run_dir: Path, pid: str, logger: logging.Logger, mode: str = 'fastq', skip_undetermined: bool = True, lanes: Optional[Set[int]] = None) -> bool:
    """
    Upload data to Nextcloud storage.

    Args:
        lims (Lims): LIMS instance
        run_dir (Path): Run directory path
        pid (str): Project ID
        logger (logging.Logger): Logger instance
        mode (str): Upload mode ('fastq' or 'bcl')
        skip_undetermined (bool): Whether to skip undetermined reads
        lanes (Optional[Set[int]]): Set of lane numbers to include

    Returns:
        True if upload succeeded
    """
    # Determine machine name
    if 'MyRun' in run_dir.parents[0].name:
        machine = run_dir.parents[1].name
    else:
        machine = run_dir.parents[0].name

    flowcell = run_dir.name.split("_")[-1]
    pid_staging = Path(Config.CONV_STAGING_DIR) / pid
    pid_staging.mkdir(parents=True, exist_ok=True)

    if mode == 'fastq':
        # Process FASTQ files
        pid_samples = set()
        pid_dir = run_dir / 'Conversion' / pid

        for fastq in pid_dir.glob('*.fastq.gz'):
            name = fastq.name.split('_')[0]
            if name != 'Undetermined':
                pid_samples.add(name)

        # Create tar files for each sample
        for sample in pid_samples:
            logger.info(f'Zipping samples for {pid}')
            sample_zip = pid_staging / f'{sample}.tar'
            sample_zip_done = pid_staging / f'{sample}.tar.done'

            if not sample_zip_done.is_file():
                os.chdir(pid_dir)
                command = f'tar -cvf {sample_zip} {sample}_*fastq.gz'

                if not run_system_command(command, logger, shell=True):
                    logger.error(f'Failed to create {sample_zip}')
                    raise TransferError(f'Failed to create tar file for sample {sample}')
                else:
                    sample_zip_done.touch()

        # Handle undetermined reads
        if not skip_undetermined:
            logger.info('Zipping undetermined reads')
            und_zip = pid_staging / 'undetermined.tar'
            und_zip_done = pid_staging / 'Undetermined.tar.done'

            if not und_zip_done.is_file():
                os.chdir(pid_dir)
                command = f'tar -cvf {und_zip} Undetermined_*fastq.gz'

                if not run_system_command(command, logger, shell=True):
                    logger.error(f'Failed to create {und_zip}')
                    raise TransferError('Failed to create undetermined tar file')
                else:
                    und_zip_done.touch()

        # Filter statistics
        logger.info(f'Filtering stats for {pid}')
        report_dir = run_dir / 'Conversion' / pid / 'Reports'
        filter_stats_by_samples(lims, pid, pid_staging, report_dir)

    else:  # BCL mode
        zipped_run = pid_staging / f'{pid}.tar'
        zip_done = pid_staging / f'{pid}.tar.done'

        if not zip_done.is_file():
            logger.info(f'Zipping {run_dir.name} to {zipped_run}')
            os.chdir(run_dir.parent)

            tar_exclude = ('--exclude "Data/" --exclude "*Conversion*" --exclude "*fastq.gz*" '
                          '--exclude "*run_zip.*" --exclude "SampleSheet*" --exclude "status.json" '
                          '--exclude "mgr.log" --exclude ".mgr_*" ')

            tar_include = f'{run_dir.name}/Data/Intensities/s.locs '

            if lanes:
                for lane in lanes:
                    if Config.DEVMODE:
                        tar_include += f'{run_dir.name}/Data/Intensities/BaseCalls/L00{lane}/C1.1/ '
                    else:
                        tar_include += f'{run_dir.name}/Data/Intensities/BaseCalls/L00{lane}/ '
            else:
                tar_include += f'{run_dir.name}/Data/Intensities/BaseCalls/'

            command = (f'tar -cvf {zipped_run} {tar_exclude} {tar_include} '
                      f'{run_dir.name}/RunInfo.xml {run_dir.name}/RunParameters.xml')

            if not run_system_command(command, logger, shell=True):
                logger.error(f'Failed to zip {run_dir.name} to {zipped_run}')
                raise TransferError(f'Failed to create BCL tar file for {pid}')
            else:
                zip_done.touch()

    # Upload to Nextcloud
    upload_id = f"{pid}_{flowcell}"
    transfer_done = Path(Config.CONV_STAGING_DIR) / f'{upload_id}.done'

    # Handle existing uploads
    global nextcloud_util
    if nextcloud_util.check_exists(upload_id) and nextcloud_util.check_exists(f'{upload_id}.done'):
        logger.info(f'Deleting previous version of {upload_id} on Nextcloud')
        nextcloud_util.delete(upload_id)
        nextcloud_util.delete(f'{upload_id}.done')
    elif nextcloud_util.check_exists(upload_id):
        logger.info(f'Deleting incomplete previous version of {upload_id} on Nextcloud')
        transfer_done.touch()

        command = f"scp -r {transfer_done} {Config.NEXTCLOUD_HOST}:{Config.NEXTCLOUD_DATA_ROOT}/{Config.NEXTCLOUD_RAW_DIR}/"
        logger.info(f'Transferring {transfer_done} to nextcloud')

        if not run_system_command(command, logger):
            logger.error(f'Failed to upload {transfer_done} to nextcloud')
            raise TransferError('Failed to upload done marker to Nextcloud')

        transfer_done.unlink()
        time.sleep(60)
        nextcloud_util.delete(upload_id)
        nextcloud_util.delete(f'{upload_id}.done')

    # Create directory on Nextcloud
    tmp_dir = run_dir / upload_id
    tmp_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f'Creating nextcloud dir for {upload_id}')

    command = f"scp -r {tmp_dir} {Config.NEXTCLOUD_HOST}:{Config.NEXTCLOUD_DATA_ROOT}/{Config.NEXTCLOUD_RAW_DIR}/"

    if not run_system_command(command, logger):
        logger.error(f'Failed to create nextcloud dir for {upload_id}')
        tmp_dir.rmdir()
        raise TransferError('Failed to create Nextcloud directory')

    tmp_dir.rmdir()

    # Create checksums
    logger.info(f'Creating md5sums for {upload_id}')
    os.chdir(pid_staging)
    command = f'md5sum *.tar > {pid_staging}/md5sums.txt'

    if not run_system_command(command, logger, shell=True):
        logger.error(f'Failed to create md5sums for files in {pid_staging}')
        raise TransferError('Failed to create checksums')

    # Upload files
    upload_commands = [
        (f'{pid_staging}/*.tar', 'tar files'),
    ]

    if mode == 'fastq':
        upload_commands.append((f'{pid_staging}/*.csv', 'CSV files'))

    upload_commands.append((f'{pid_staging}/*.txt', 'text files'))

    for file_pattern, description in upload_commands:
        logger.info(f'Transferring {description} to nextcloud')
        command = f"scp -r {file_pattern} {Config.NEXTCLOUD_HOST}:{Config.NEXTCLOUD_DATA_ROOT}/{Config.NEXTCLOUD_RAW_DIR}/{upload_id}"

        if not run_system_command(command, logger, shell=True):
            logger.error(f'Failed to upload {description} to nextcloud')
            raise TransferError(f'Failed to upload {description} to Nextcloud')

    # Mark transfer as complete
    transfer_done.touch()
    command = f"scp -r {transfer_done} {Config.NEXTCLOUD_HOST}:{Config.NEXTCLOUD_DATA_ROOT}/{Config.NEXTCLOUD_RAW_DIR}/"
    logger.info(f'Transferring {transfer_done} to nextcloud')

    if not run_system_command(command, logger):
        logger.error(f'Failed to upload {transfer_done} to nextcloud')
        raise TransferError('Failed to upload completion marker to Nextcloud')
    else:
        logger.info(f'Cleaning up {pid_staging}')
        for file in pid_staging.iterdir():
            try:
                file.unlink()
            except OSError as e:
                logger.warning(f"Could not delete {file}: {e}")

    return True


def send_status_mail(lims: Lims, message: str, run_dir: Path, project_data: Dict[str, Any], run_data: Dict[str, Any]):
    """
    Send status email with run information and attachments.

    Args:
        lims (Lims): LIMS instance
        message (str): Status message
        run_dir (Path): Run directory path
        project_data (Dict[str, Any]): Project information
        run_data (Dict[str, Any]): Run information
    """
    status_file = run_dir / 'status.json'
    log_file = run_dir / 'mgr.log'
    summary_stats_file = run_dir / 'Conversion' / 'Reports' / f'{run_dir.name}_summary.csv'
    demux_stats = run_dir / 'Conversion' / 'Reports' / 'Demultiplex_Stats.csv'

    # Potential attachment files
    plot_files = [
        (run_dir / 'Conversion' / 'Reports' / f'{run_dir.name}_BasePercent-by-cycle_BasePercent.png', 'basepercent_by_cycle_plot'),
        (run_dir / 'Conversion' / 'Reports' / f'{run_dir.name}_Intensity-by-cycle_Intensity.png', 'intensity_by_cycle_plot'),
        (run_dir / 'Conversion' / 'Reports' / f'{run_dir.name}_Clusters-by-lane.png', 'clusterdensity_by_lane_plot'),
        (run_dir / 'Conversion' / 'Reports' / f'{run_dir.name}_flowcell-Intensity.png', 'flowcell_intensity_plot'),
        (run_dir / 'Conversion' / 'Reports' / f'{run_dir.name}_q-heat-map.png', 'q_heatmap_plot'),
        (run_dir / 'Conversion' / 'Reports' / f'{run_dir.name}_q-histogram.png', 'q_histogram_plot'),
    ]

    # Read status and log files
    status = {}
    log = ""

    if status_file.is_file():
        with open(status_file, 'r') as s:
            status = json.load(s)

    if log_file.is_file():
        with open(log_file, 'r') as l:
            log = l.read()

    # Get expected reads and yields
    run_params_file = run_dir / 'RunParameters.xml'
    run_info_file = run_dir / 'RunInfo.xml'

    expected_reads = 0
    expected_yields = {'r1': 0, 'r2': 0}

    if run_params_file.is_file():
        expected_reads = get_expected_reads(str(run_params_file))

    if run_info_file.is_file() and expected_reads:
        expected_yields = get_expected_yield(run_info_file, expected_reads)

    # Parse statistics
    conversion_stats = {}
    summary_stats = {}

    if demux_stats.is_file():
        conversion_stats = parse_conversion_stats(run_dir / 'Conversion' / 'Reports')

    if summary_stats_file.is_file():
        summary_stats = parse_summary_stats(summary_stats_file)

    # Prepare attachments
    attachments = {}
    for plot_path, plot_key in plot_files:
        if plot_path.is_file():
            attachments[plot_key] = str(plot_path)
        else:
            attachments[plot_key] = None

    # Get project names
    project_names = {}
    for pid in project_data:
        try:
            project = Project(lims, id=pid)
            project_names[pid] = project.name
        except Exception as e:
            logger.warning(f"Could not get project name for {pid}: {e}")
            project_names[pid] = pid

    status_txt = [f'{pid}:{project_names[pid]}' for pid in project_names]

    # Prepare template data
    template_data = {
        'status': status,
        'log': log,
        'projects': ",".join(status_txt),
        'run_dir': run_dir.name,
        'nr_reads': f'{conversion_stats.get("total_reads", 0):,.0f} / {expected_reads:,}' if expected_reads else '0',
        'expected_yields': expected_yields,
        'conversion_stats': conversion_stats,
        'summary_stats': summary_stats,
        'run_data': run_data,
    }

    # Generate email content and send
    mail_content = render_template('conversion_status_mail.html', template_data)

    if status_txt:
        mail_subject = f'[USEQ] Status ({", ".join(status_txt)}): {message}'
    else:
        mail_subject = f'[USEQ] Status ({run_dir.name}): {message}'

    send_mail(mail_subject, mail_content, Config.MAIL_SENDER, Config.MAIL_ADMINS, attachments=attachments)


def process_run_directory(lims: Lims, run_dir: Path, machine: str, logger: logging.Logger):
    """
    Process a single run directory through the complete pipeline.

    Args:
        lims (Lims): LIMS instance
        run_dir (Path): Run directory path
        machine (str): Machine name
        logger (logging.Logger): Logger instance
    """
    # Important file paths
    sample_sheet = run_dir / 'SampleSheet.csv'
    rta_complete = run_dir / 'RTAComplete.txt'
    running_file = run_dir / '.mgr_running'
    failed_file = run_dir / '.mgr_failed'
    done_file = run_dir / '.mgr_done'
    status_file = run_dir / 'status.json'

    # Check if run should be processed
    if not (rta_complete.is_file() and
            not any(f.is_file() for f in [running_file, failed_file, done_file])):
        return

    # Lock directory for processing
    running_file.touch()

    try:
        # Parse run information
        run_info = xml.dom.minidom.parse(str(run_dir / 'RunInfo.xml'))
        first_tile = run_info.getElementsByTagName('Tile')[0].firstChild.nodeValue.split("_")[-1]

        flowcell_layout = run_info.getElementsByTagName('FlowcellLayout')[0]
        default_lanes = int(flowcell_layout.getAttribute('LaneCount'))
        default_lanes = set(range(1, default_lanes + 1))

        # Initialize status
        status = {
            'run': {
                'Stats': False,
                'Archive': False,
            },
            'projects': {},
        }

        # Load existing status if available
        if status_file.is_file():
            logger.info('Found existing status file')
            with open(status_file, 'r') as s:
                status = json.load(s)
                logger.info(f'Current status: {json.dumps(status, indent=2)}')

        flowcell = run_dir.name.split("_")[-1]

        # Find or retrieve sample sheet
        if not sample_sheet.is_file():
            sample_sheet = find_or_retrieve_sample_sheet(run_dir, logger)
            if not sample_sheet:
                logger.error('Failed to find SampleSheet.csv')
                raise RunManagerError('Sample sheet not found')

        # Parse sample sheet and organize data
        logger.info('Extracting sample/project information from samplesheet')
        run_data, project_data = parse_run_data(sample_sheet, default_lanes)

        # Process each project
        for pid in project_data:
            if pid not in status['projects']:
                status['projects'][pid] = {
                    'BCL-Only': False,
                    'Demultiplexing': False,
                    'Transfer-nc': False,
                    'Transfer-hpc': False,
                }
                update_status(status_file, status)

            # Demultiplexing
            if not status['projects'][pid]['Demultiplexing'] and not status['projects'][pid]['BCL-Only']:
                logger.info(f'Starting demultiplexing attempt for projectID {pid}')

                try:
                    success = demultiplex_project(run_dir, pid, project_data[pid],
                                                run_data, parse_sample_sheet(sample_sheet),
                                                first_tile, logger)
                    status['projects'][pid]['Demultiplexing'] = success
                    if not success:
                        status['projects'][pid]['BCL-Only'] = True
                    update_status(status_file, status)

                except Exception as e:
                    logger.error(f'Demultiplexing failed for {pid}: {e}')
                    raise

            # Determine transfer mode
            transfer_mode = 'fastq' if status['projects'][pid]['Demultiplexing'] else 'bcl'

            # Nextcloud transfer
            if not status['projects'][pid]['Transfer-nc']:
                skip_undetermined = any(len(run_data['lanes'][lane]['projects']) > 1
                                      for lane in project_data[pid]['on_lanes'])

                if skip_undetermined:
                    logger.info(f'Skipping upload of undetermined reads for {pid}')

                try:
                    success = upload_to_nextcloud(lims, run_dir, pid, logger,
                                                mode=transfer_mode,
                                                skip_undetermined=skip_undetermined,
                                                lanes=project_data[pid]['on_lanes'])
                    status['projects'][pid]['Transfer-nc'] = success
                    update_status(status_file, status)

                except Exception as e:
                    logger.error(f'Nextcloud transfer failed for {pid}: {e}')
                    raise

            # HPC transfer
            if not status['projects'][pid]['Transfer-hpc']:
                try:
                    success = upload_to_hpc(lims, run_dir, pid, logger)
                    status['projects'][pid]['Transfer-hpc'] = success
                    update_status(status_file, status)

                except Exception as e:
                    logger.error(f'HPC transfer failed for {pid}: {e}')
                    raise

        # Generate run statistics
        if not status['run']['Stats']:
            try:
                success = generate_run_statistics(lims, run_dir, logger)
                status['run']['Stats'] = success
                update_status(status_file, status)

            except Exception as e:
                logger.error(f'Failed to create conversion statistics: {e}')
                raise

        # Archive run
        if not status['run']['Archive']:
            try:
                success = upload_to_archive(run_dir, logger)
                status['run']['Archive'] = success
                update_status(status_file, status)

            except Exception as e:
                logger.error(f'Failed to archive run: {e}')
                raise

        # Check if everything is complete
        if (status['run']['Stats'] and status['run']['Archive'] and
            all(status['projects'][pid]['Transfer-nc'] and status['projects'][pid]['Transfer-hpc']
                for pid in project_data)):

            cleanup_fastq_files(run_dir, logger)
            send_status_mail(lims, 'Finished', run_dir, project_data, run_data)
            running_file.unlink()
            done_file.touch()
            logger.info('Run processing completed successfully')

    except Exception as e:
        logger.error(f'Run processing failed: {e}')
        logger.error(traceback.format_exc())
        running_file.unlink()
        failed_file.touch()

        # Try to send failure notification
        try:
            send_status_mail(lims, 'Failed (see logs)', run_dir,
                           project_data if 'project_data' in locals() else {},
                           run_data if 'run_data' in locals() else {})
        except Exception as mail_error:
            logger.error(f'Failed to send status mail: {mail_error}')


def find_or_retrieve_sample_sheet(run_dir: Path, logger: logging.Logger) -> Optional[Path]:
    """
    Find existing sample sheet or retrieve from LIMS.

    Args:
        run_dir (Path): Run directory path
        logger (logging.Logger): Logger instance

    Returns:
        Path to sample sheet if found, None otherwise
    """
    sample_sheet = run_dir / 'SampleSheet.csv'

    # Check for existing sample sheet with different name
    csv_files = list(run_dir.glob("*.csv"))
    if csv_files:
        existing_sheet = csv_files[0]
        logger.info(f'Found {existing_sheet}, renaming to SampleSheet.csv')
        existing_sheet.rename(sample_sheet)
        return sample_sheet

    # Try to retrieve from LIMS
    logger.info('Trying to find SampleSheet.csv in LIMS')

    try:
        # Get container name from RunParameters.xml
        run_parameters_old = run_dir / 'runParameters.xml'
        run_parameters = run_dir / 'RunParameters.xml'

        if run_parameters_old.is_file():
            run_parameters_old.rename(run_parameters)

        if not run_parameters.is_file():
            logger.error('RunParameters.xml not found')
            return None

        run_params = xml.dom.minidom.parse(str(run_parameters))
        lims_container_name = None

        # Try different XML elements based on machine type
        container_elements = [
            'ReagentKitSerial',  # NextSeq
            'LibraryTubeSerialBarcode',  # NovaSeq
            'SerialNumber'  # iSeq - use second occurrence
        ]

        for element_name in container_elements:
            elements = run_params.getElementsByTagName(element_name)
            if elements:
                if element_name == 'SerialNumber' and len(elements) > 1:
                    lims_container_name = elements[1].firstChild.nodeValue
                elif element_name != 'SerialNumber':
                    lims_container_name = elements[0].firstChild.nodeValue
                break

        if not lims_container_name:
            logger.error('Could not determine container name from RunParameters.xml')
            return None

        logger.info(f'Container name: {lims_container_name}')

        # Search LIMS for sample sheet
        for reagent_kit_artifact in lims.get_artifacts(containername=lims_container_name):
            process = reagent_kit_artifact.parent_process

            for artifact in process.result_files():
                if 'SampleSheet' in artifact.name and artifact.files:
                    file_id = artifact.files[0].id
                    sample_sheet_content = lims.get_file_contents(id=file_id)

                    with open(sample_sheet, 'w') as sample_sheet_file:
                        sample_sheet_file.write(sample_sheet_content)

                    logger.info(f'Retrieved sample sheet from LIMS: {sample_sheet}')
                    return sample_sheet

        logger.error('Sample sheet not found in LIMS')
        return None

    except Exception as e:
        logger.error(f'Error retrieving sample sheet from LIMS: {e}')
        return None


def parse_run_data(sample_sheet: Path, default_lanes: Set[int]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Parse run data from sample sheet.

    Args:
        sample_sheet (Path): Path to sample sheet
        default_lanes (Set[int]): Set of default lane numbers

    Returns:
        Tuple of (run_data, project_data)
    """
    run_data = {'lanes': {}}
    project_data = {}

    # Initialize lanes
    for lane in default_lanes:
        run_data['lanes'][lane] = {
            'projects': set(),
            'samples': [],
        }

    # Parse sample sheet
    master_sheet = parse_sample_sheet(sample_sheet)

    for sample in master_sheet['samples']:
        pid = sample[master_sheet['header'].index('Sample_Project')]

        if pid not in project_data:
            project_data[pid] = {
                'sample_ids': set(),
                'samples': [],
                'on_lanes': set()
            }

        project_data[pid]['samples'].append(sample)
        project_data[pid]['sample_ids'].add(sample[master_sheet['header'].index('Sample_ID')])

        # Handle lane assignments
        if 'Lane' in master_sheet['header']:
            lane = int(sample[master_sheet['header'].index('Lane')])
            project_data[pid]['on_lanes'].add(lane)

            if lane not in run_data['lanes']:
                run_data['lanes'][lane] = {
                    'projects': set(),
                    'samples': []
                }

            run_data['lanes'][lane]['projects'].add(pid)
            run_data['lanes'][lane]['samples'].append(pid)

    # Handle projects without specific lane assignments
    for lane in run_data['lanes']:
        if not run_data['lanes'][lane]['projects'] and 'Lane' not in master_sheet['header']:
            for pid in project_data:
                run_data['lanes'][lane]['projects'].add(pid)
                run_data['lanes'][lane]['samples'].extend(project_data[pid]['samples'])
                project_data[pid]['on_lanes'].add(lane)

    return run_data, project_data


def setup_logger(run_dir: Path) -> logging.Logger:
    """
    Set up logger for run processing.

    Args:
        run_dir (Path): Run directory path

    Returns:
        Configured logger instance
    """
    log_file = run_dir / 'mgr.log'

    logger = logging.getLogger('Run_Manager')
    logger.setLevel(logging.DEBUG)

    # Clear existing handlers
    logger.handlers.clear()

    # Set up file log handler
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.INFO)

    # Set up console log handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # Create and add formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)

    # Add handlers to logger
    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


def manage_runs(lims: Lims) -> None:
    """Main function to manage sequencing runs.

    Args:
        lims (Lims): LIMS instance
    """
    machine_aliases = Config.MACHINE_ALIASES
    if Config.DEVMODE:
        machine_aliases = ['novaseqx_01']  # Only used for dev runs

    for machine in machine_aliases:
        machine_dir = Path(Config.CONV_MAIN_DIR) / machine

        if not machine_dir.exists():
            continue

        for run_dir in machine_dir.iterdir():
            # Validate run directory format
            if not run_dir.is_dir() or run_dir.name.count('_') != 3:
                continue

            # Set up logging for this run
            logger = setup_logger(run_dir)

            try:
                logger.info(f'Processing run directory: {run_dir.name}')
                process_run_directory(lims, run_dir, machine, logger)

            except Exception as e:
                logger.error(f'Fatal error processing {run_dir.name}: {e}')
                logger.error(traceback.format_exc())

            finally:
                # Clean up logger handlers to prevent memory leaks
                for handler in logger.handlers[:]:
                    handler.close()
                    logger.removeHandler(handler)


def zip_conversion_report(run_dir: Path, logger: logging.Logger) -> str:
    """Create zip file of conversion reports.

    Args:
        run_dir: Run directory path
        logger: Logger instance

    Returns:
        Path to created zip file
    """
    logger.info('Zipping conversion report')
    zip_file = run_dir / f'{run_dir.name}_Reports.zip'

    conversion_dir = run_dir / 'Conversion'
    if not conversion_dir.exists():
        raise RunManagerError(f'Conversion directory not found: {conversion_dir}')

    os.chdir(conversion_dir)
    command = f'zip -FSr {zip_file} Reports/'

    if not run_system_command(command, logger):
        logger.error('Failed to zip conversion report')
        raise RunManagerError('Failed to create conversion report zip')

    return str(zip_file)


def run(lims: Lims):
    """Main entry point for run management.

    Args:
        lims (Lims): LIMS connection object
    """
    # Set up Nextcloud connection
    global nextcloud_util
    try:
        nextcloud_util = NextcloudUtil()
        nextcloud_util.set_hostname(Config.NEXTCLOUD_HOST)
        nextcloud_util.setup(
            Config.NEXTCLOUD_USER,
            Config.NEXTCLOUD_PW,
            Config.NEXTCLOUD_WEBDAV_ROOT,
            Config.NEXTCLOUD_RAW_DIR,
            Config.MAIL_SENDER
        )

        # Run the main management function
        manage_runs(lims)

    except Exception as e:
        # Log to a general log file if specific run logging isn't available
        general_logger = logging.getLogger('Run_Manager_General')
        general_logger.error(f'Fatal error in run management: {e}')
        general_logger.error(traceback.format_exc())
        raise
