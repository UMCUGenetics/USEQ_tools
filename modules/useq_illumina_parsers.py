import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Optional, Any, Union

# Assuming Config is available in the environment
from config import Config


def parse_conversion_stats(conversion_stats_file: Union[str, Path]) -> Optional[Dict[str, Any]]:
    """
    Parses the Illumina ConversionStats.xml file to aggregate yield and Q30 metrics.

    Args:
        conversion_stats_file (Union[str, Path]): Path to the XML file.

    Returns:
        A dictionary containing parsed statistics or None if file does not exist.
    """
    file_path = Path(conversion_stats_file)
    if not file_path.is_file():
        return None

    # Initialize data structure
    conversion_stats = {
        'samples': {},
        'unknown': {},
        'total_reads': 0,
        'total_reads_raw': 0
    }

    # Use iterparse for memory efficiency on large XML files
    context = ET.iterparse(file_path, events=("end",))

    for _, elem in context:
        if elem.tag == 'Sample':
            sample_name = elem.get('name')

            # Skip "all" or invalid samples
            if not sample_name or sample_name == "all":
                continue

            barcode = elem.find('Barcode')
            if barcode is None:
                continue

            barcode_name = barcode.get('name')
            if barcode_name and "N" in barcode_name:
                continue

            # Initialize sample dict
            if sample_name not in conversion_stats['samples']:
                conversion_stats['samples'][sample_name] = {
                    'barcode': barcode_name,
                    'qsum': 0,
                    'yield': 0,
                    'yield_Q30': 0,
                    'cluster_count': 0,
                    'mean_quality': 0,
                    'percent_Q30': 0
                }

            current_sample = conversion_stats['samples'][sample_name]

            for lane in barcode.findall("Lane"):
                # Temporary storage for this lane's calculation
                lane_metrics = {
                    'r1': {'yield': 0, 'yield_Q30': 0, 'qscore_sum': 0},
                    'r2': {'yield': 0, 'yield_Q30': 0, 'qscore_sum': 0}
                }

                for tile in lane.findall("Tile"):
                    raw_counts = tile.find("Raw")
                    pf_counts = tile.find("Pf")

                    if raw_counts is None or pf_counts is None:
                        continue

                    # Update global and sample cluster counts
                    pf_cluster_count = int(pf_counts.findtext("ClusterCount", "0"))
                    raw_cluster_count = int(raw_counts.findtext("ClusterCount", "0"))

                    current_sample['cluster_count'] += pf_cluster_count
                    conversion_stats['total_reads'] += pf_cluster_count
                    conversion_stats['total_reads_raw'] += raw_cluster_count

                    for read in pf_counts.findall("Read"):
                        read_number = read.get("number")
                        if not read_number or int(read_number) > 2:
                            continue

                        read_key = f'r{read_number}'
                        lane_metrics[read_key]['yield'] += int(read.findtext("Yield", "0"))
                        lane_metrics[read_key]['yield_Q30'] += int(read.findtext("YieldQ30", "0"))
                        lane_metrics[read_key]['qscore_sum'] += int(read.findtext("QualityScoreSum", "0"))

                # Aggregate Lane metrics into Sample metrics
                for r_key in ['r1', 'r2']:
                    current_sample['qsum'] += lane_metrics[r_key]['qscore_sum']
                    current_sample['yield'] += lane_metrics[r_key]['yield']
                    current_sample['yield_Q30'] += lane_metrics[r_key]['yield_Q30']

            # Calculate percentages and averages
            total_yield = float(current_sample['yield'])
            if total_yield > 0:
                p_q30 = (current_sample['yield_Q30'] / total_yield) * 100
                mean_q = current_sample['qsum'] / total_yield
                current_sample['percent_Q30'] = f"{p_q30:.2f}"
                current_sample['mean_quality'] = f"{mean_q:.2f}"
            else:
                current_sample['percent_Q30'] = "0.00"
                current_sample['mean_quality'] = "0.00"

            # Format cluster count with commas
            current_sample['cluster_count'] = f"{current_sample['cluster_count']:,}"

            # Clear element to free memory
            elem.clear()

        elif elem.tag == 'TopUnknownBarcodes':
            for barcode in elem.findall("Barcode"):
                bc_count = int(barcode.get("count", 0))
                bc_seq = barcode.get("sequence")

                if bc_seq:
                    conversion_stats['unknown'][bc_seq] = conversion_stats['unknown'].get(bc_seq, 0) + bc_count

            elem.clear()

    # Format unknown counts with commas
    for bc in conversion_stats['unknown']:
        conversion_stats['unknown'][bc] = f"{conversion_stats['unknown'][bc]:,}"

    return conversion_stats


def get_expected_reads(run_parameters_file: Union[str, Path]) -> Optional[int]:
    """
    Parses RunParameters.xml to determine expected yield based on configuration.

    Args:
        run_parameters_file (Union[str, Path]): Path to the XML file.

    Returns:
        Expected reads count (int) or None if file missing.
    """
    file_path = Path(run_parameters_file)
    if not file_path.is_file():
        return None

    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
    except ET.ParseError:
        return None

    # Helper to safely extract text from XML tags
    def get_tag_value(tag_name: str) -> str:
        # Search recursively for the tag
        node = root.find(f".//{tag_name}")
        return node.text if node is not None and node.text else ''

    run_chem = get_tag_value('Chemistry')
    run_version = get_tag_value('ReagentKitVersion')
    flowcell_mode = get_tag_value('FlowCellMode')

    application_name = get_tag_value('ApplicationName')
    if not application_name:
        application_name = get_tag_value('RecipeName')

    # Determine expected reads based on priority
    if run_chem in Config.RUNTYPE_YIELDS:
        return Config.RUNTYPE_YIELDS[run_chem]
    elif run_version in Config.RUNTYPE_YIELDS:
        return Config.RUNTYPE_YIELDS[run_version]
    elif flowcell_mode in Config.RUNTYPE_YIELDS:
        return Config.RUNTYPE_YIELDS[flowcell_mode]
    elif application_name in Config.RUNTYPE_YIELDS:
        return Config.RUNTYPE_YIELDS[application_name]

    return Config.RUNTYPE_YIELDS.get('HiSeq rapid')


def parse_sample_sheet(sample_sheet_path: Union[str, Path]) -> Dict[str, Any]:
    """
    Parses a CSV-style Illumina Sample Sheet.

    Args:
        sample_sheet_path (Union[str, Path]): Path to the sample sheet file.

    Returns:
        Dictionary containing header, samples list, and top section metadata.
    """
    data = {'top': '', 'samples': [], 'header': []}

    try:
        with open(sample_sheet_path, 'r') as sheet:
            header_found = False

            for line in sheet:
                line = line.rstrip()

                if 'Sample_ID' in line:
                    data['header'] = line.split(',')
                    header_found = True
                    continue

                if header_found and line:
                    # Assuming CSV format
                    data['samples'].append(line.split(','))
                else:
                    data['top'] += f"{line}\n"

    except FileNotFoundError:
        print(f"Error: Sample sheet not found at {sample_sheet_path}")
        return data

    return data
