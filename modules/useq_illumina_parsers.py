import os
import xml.etree.cElementTree as ET
from xml.dom.minidom import parseString, parse
from config import RUNTYPE_YIELDS

def parseConversionStats( conversion_stats ):

    if not os.path.isfile( conversion_stats ):
        return None

    tree = ET.ElementTree(file=conversion_stats)
    conversion_stats = {'samples':{}, 'unknown':{}, 'total_reads' : 0, 'total_reads_raw' : 0}
    #
    paired_end = False
    for elem in tree.iter(tag='Sample'):

        sample_name = elem.attrib['name']

        if sample_name != "all":
            barcode = elem.find('Barcode')
            barcode_name = barcode.attrib['name']
            if "N" in barcode_name:
                continue

            conversion_stats['samples'][ sample_name ] = {}
            conversion_stats['samples'][ sample_name] = {'barcode':barcode_name, 'qsum':0,'yield':0,'yield_Q30':0,'cluster_count':0, 'mean_quality':0, 'percent_Q30':0}
            for lane in barcode.findall("Lane"):
                lane_nr = lane.attrib["number"]
                lane_counts = {
                    'pf' : {'r1':{'yield':0,'yield_Q30':0,'qscore_sum':0}, 'r2':{'yield':0,'yield_Q30':0,'qscore_sum':0}}
                }
                for tile in lane.findall("Tile"):

                    tile_nr = tile.attrib["number"]
                    raw_counts = tile.find("Raw")
                    pf_counts = tile.find("Pf")

                    conversion_stats['samples'][ sample_name]['cluster_count'] += int(pf_counts.find("ClusterCount").text)
                    conversion_stats['total_reads'] += int(pf_counts.find("ClusterCount").text)
                    conversion_stats['total_reads_raw'] += int(raw_counts.find("ClusterCount").text)

                    for read in pf_counts.findall("Read"):
                        read_number = read.attrib["number"]
                        lane_counts['pf']['r'+str(read_number)]['yield'] += int(read.find("Yield").text)
                        lane_counts['pf']['r'+str(read_number)]['yield_Q30'] += int(read.find("YieldQ30").text)
                        lane_counts['pf']['r'+str(read_number)]['qscore_sum'] += int(read.find("QualityScoreSum").text)

                    conversion_stats['samples'][ sample_name]['qsum'] += lane_counts['pf']['r1']['qscore_sum']
                    conversion_stats['samples'][ sample_name]['qsum'] += lane_counts['pf']['r2']['qscore_sum']
                    conversion_stats['samples'][ sample_name]['yield'] += lane_counts['pf']['r1']['yield']
                    conversion_stats['samples'][ sample_name]['yield'] += lane_counts['pf']['r2']['yield']
                    conversion_stats['samples'][ sample_name]['yield_Q30'] += lane_counts['pf']['r1']['yield_Q30']
                    conversion_stats['samples'][ sample_name]['yield_Q30'] += lane_counts['pf']['r2']['yield_Q30']


            if float( conversion_stats['samples'][ sample_name ]['yield'] ):
                conversion_stats['samples'][ sample_name ]['percent_Q30'] = "{0:.2f}".format( (conversion_stats['samples'][ sample_name ]['yield_Q30'] / float(conversion_stats['samples'][ sample_name ]['yield']))*100 )
                conversion_stats['samples'][ sample_name ]['mean_quality'] = "{0:.2f}".format( conversion_stats['samples'][ sample_name ]['qsum'] / float(conversion_stats['samples'][ sample_name ]['yield']) )
            else:
                conversion_stats['samples'][ sample_name ]['percent_Q30'] = "{0:.2f}".format(0)
                conversion_stats['samples'][ sample_name ]['mean_quality'] = "{0:.2f}".format(0)

            conversion_stats['samples'][ sample_name ]['cluster_count'] = "{0:,}".format( conversion_stats['samples'][ sample_name]['cluster_count'] )

    for top_unknown in tree.findall("TopUnknownBarcodes"):
        for barcode in top_unknown.findall("Barcode"):
            bc_count = int(barcode.attrib["count"])
            bc_seq = barcode.attrib["sequence"]
            if bc_seq in conversion_stats['unknown']:
                conversion_stats['unknown'][bc_seq] += bc_count
            else:
                conversion_stats['unknown'][bc_seq] = bc_count

    for bc in conversion_stats['unknown']:
        conversion_stats['unknown'][bc] = "{0:,}".format(conversion_stats['unknown'][bc])

    return conversion_stats



def parseRunParameters( run_parameters):
    if not os.path.isfile( run_parameters ):
        return None

    run_parameters = parse(run_parameters)
    expected_reads = None
    try:
        run_chem = run_parameters.getElementsByTagName('Chemistry')[0].firstChild.nodeValue
    except:
        run_chem = ''
    try:
        run_version = run_parameters.getElementsByTagName('ReagentKitVersion')[0].firstChild.nodeValue
    except:
        run_version = ''
    if run_chem in RUNTYPE_YIELDS:
        expected_reads = RUNTYPE_YIELDS[run_chem]
    elif run_version in RUNTYPE_YIELDS:
        expected_reads = RUNTYPE_YIELDS[run_version]
    else:
        expected_reads = RUNTYPE_YIELDS['HiSeq rapid']

    return expected_reads
