import os
import xml.etree.cElementTree as ET
from xml.dom.minidom import parseString, parse
from config import Config

def parseConversionStats( conversion_stats_file ):

    if not os.path.isfile( conversion_stats_file ):
        return None

    # tree = ET.ElementTree(file=conversion_stats)
    # parser = ET.iterparse(conversion_stats)
    conversion_stats = {'samples':{}, 'unknown':{}, 'total_reads' : 0, 'total_reads_raw' : 0}
    #
    paired_end = False
    for event, elem in ET.iterparse(conversion_stats_file):

        if elem.tag == 'Sample':
            sample_name = elem.attrib['name']
            print(sample_name)
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
                            if int(read_number) > 2: continue
                            print(tile_nr, read_number, read.find("Yield").text, read.find("YieldQ30").text, read.find("QualityScoreSum").text)
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

            elem.clear()
        elif elem.tag =='TopUnknownBarcodes':
            for barcode in elem.findall("Barcode"):

                bc_count = int(barcode.attrib["count"])
                bc_seq = barcode.attrib["sequence"]

                if bc_seq in conversion_stats['unknown']:
                    conversion_stats['unknown'][bc_seq] += bc_count
                else:
                    conversion_stats['unknown'][bc_seq] = bc_count

            elem.clear()
    # for top_unknown in tree.findall("TopUnknownBarcodes"):

    # for event, elem in ET.iterparse(conversion_stats_file):
    #     if elem.tag =='TopUnknownBarcodes':
    #         for barcode in elem.findall("Barcode"):
    #             bc_count = int(barcode.attrib["count"])
    #             bc_seq = barcode.attrib["sequence"]
    #             if bc_seq in conversion_stats['unknown']:
    #                 conversion_stats['unknown'][bc_seq] += bc_count
    #             else:
    #                 conversion_stats['unknown'][bc_seq] = bc_count
    #         elem.clear()
    for bc in conversion_stats['unknown']:

        conversion_stats['unknown'][bc] = "{0:,}".format(conversion_stats['unknown'][bc])
    print(conversion_stats['unknown'])

    return conversion_stats

def getExpectedReads( run_parameters):
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

    try:
        flowcell_mode = run_parameters.getElementsByTagName('FlowCellMode')[0].firstChild.nodeValue
    except:
        flowcell_mode = ''

    if run_chem in Config.RUNTYPE_YIELDS:
        expected_reads = Config.RUNTYPE_YIELDS[run_chem]
    elif run_version in Config.RUNTYPE_YIELDS:
        expected_reads = Config.RUNTYPE_YIELDS[run_version]
    elif flowcell_mode in Config.RUNTYPE_YIELDS:
        expected_reads = Config.RUNTYPE_YIELDS[flowcell_mode]
    else:
        expected_reads = Config.RUNTYPE_YIELDS['HiSeq rapid']

    return expected_reads

def parseSampleSheet(sample_sheet):
    data = {'top': '', 'samples': [], 'header':[] }
    with open(sample_sheet) as sheet:
        header = None

        for line in sheet.readlines():
            line = line.rstrip()
            if 'Sample_ID' in line:
                header = line.rstrip().split(',')
                data['header'] = header
                continue
            elif header:
                sample = line.split(",")
                data['samples'].append(sample)
            else:
                data['top'] += f"{line}\n"
        return data
