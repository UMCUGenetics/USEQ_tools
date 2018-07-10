import argparse
# from optparse import OptionParser
import glsapiutil
import sys
from xml.dom.minidom import parseString, parse

HOSTNAME = ''
VERSION = ''
BASE_URI = ''

DEBUG = True
api = None
args = None
CACHE = {}

def getObjectDOM( uri ):

	global CACHE
	#global api
	if uri not in CACHE.keys():

		thisXML = api.getResourceByURI( uri )

		thisDOM = parseString( thisXML )
		CACHE[ uri ] = thisDOM

	return CACHE[ uri ]


def setupGlobalsFromURI( uri ):

	global HOSTNAME
	global VERSION
	global BASE_URI

	tokens = uri.split( "/" )
	HOSTNAME = "/".join(tokens[0:3])
	VERSION = tokens[4]
	BASE_URI = "/".join(tokens[0:5]) + "/"

	if DEBUG is True:
		print HOSTNAME
		print BASE_URI

def revcom(step, aid, output_file):

    revcompl = lambda x: ''.join([{'A':'T','C':'G','G':'C','T':'A'}[B] for B in x][::-1])
    samplesheet_artifact = getObjectDOM(BASE_URI + 'artifacts/' + aid)
    samplesheet_uri = samplesheet_artifact.getElementsByTagName("file:file")[0].getAttribute("uri")
    header = ''
    for line in api.getResourceByURI(samplesheet_uri + '/download').rstrip().split('\n'):
        if line.startswith('Sample_ID'):
            header = line.rstrip().split(',')
            output_file.write('{line}\n'.format(line=line))
            # print '{line}\n'.format(line=line)
        elif header:
            data = line.rstrip().split(',')

            i7_index = None
            i5_index = None
            if 'index2' in header:
                i5_index =header.index('index2')
                data[i5_index] = revcompl(data[i5_index])
            else:
                i7_index = header.index('index')
                data[i7_index] = revcompl(data[i7_index])

            output_file.write('{line}\n'.format(line=','.join(data)))

        else:
            output_file.write('{line}\n'.format(line=line))


def main():

    global args
    global api

    parser = argparse.ArgumentParser(prog='useq_modify_samplesheet', description='''
    This script is used to modify a samplesheet to work with either NextSeq or MiSeq/HiSeq. Currently
    all it does is reverse complement the barcodes when needed''')


    parser.add_argument('-u','--user', help='User name', required=True)
    parser.add_argument('-p','--password', help='Password', required=True)

    parser.add_argument('-s','--step', help='Step URI', required=True)
    parser.add_argument('-a','--aid', help='Artifact ID', required=True)
    parser.add_argument('-o','--output_file',  nargs='?', type=argparse.FileType('w'), default=sys.stdout, help='Output file path (default=stdout)')

    args = parser.parse_args()


    setupGlobalsFromURI( args.step )
    api = glsapiutil.glsapiutil()
    api.setHostname( HOSTNAME )
    api.setVersion( VERSION )
    api.setup( args.user, args.password )


    revcom(args.step, args.aid, args.output_file)

if __name__ == "__main__":
    main()



#
# def update_samplesheet(lims, process_id, artifact_id, output_file):
#     """Update illumina samplesheet."""
#     process = Process(lims, id=process_id)
#     sample_project = {}
#     for artifact in process.all_inputs():
#         for sample in artifact.samples:
#             if sample.udf['Dx NICU Spoed']:
#                 sample_project[sample.name] = 'NICU_{0}'.format(sample.udf['Dx Familienummer'])
#             elif sample.udf['Dx Protocolomschrijving'] == 'Exoom.analy_IL_elidS30409818_Exoomver.':
#                 sample_project[sample.name] = 'CREv2'
#             else:
#                 sample_project[sample.name] = 'Unkown_project'
#
#     header = ''  # empty until [data] section
#
#     samplesheet_artifact = Artifact(lims, id=artifact_id)
#     file_id = samplesheet_artifact.files[0].id
#     for line in lims.get_file_contents(id=file_id).rstrip().split('\n'):
#         if line.startswith('Sample_ID'):
#             header = line.rstrip().split(',')
#             output_file.write('{line}\n'.format(line=line))
#         elif header:
#             data = line.rstrip().split(',')
#             sample_id_index = header.index('Sample_ID')
#             sample_name_index = header.index('Sample_Name')
#             sample_project_index = header.index('Sample_Project')
#
#             # Set Sample_Project
#             sample_name = re.search('^U\d+[CP][MFO](\w+)$', data[sample_name_index]).group(1)
#             data[sample_project_index] = sample_project[sample_name]
#
#             # Overwrite Sample_ID with Sample_name to get correct conversion output folder structure
#             data[sample_id_index] = data[sample_name_index]
#
#             output_file.write('{line}\n'.format(line=','.join(data)))
#         else:
#             output_file.write('{line}\n'.format(line=line))
