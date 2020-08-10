from genologics.entities import Artifact
from modules.useq_template import TEMPLATE_PATH,TEMPLATE_ENVIRONMENT,renderTemplate

def reverseComplement(lims, step_uri, aid, output_file):
	"""Reverse complements the i5 index for dual-barcodes index sequences and the i7 index for single index sequences"""
	revcompl = lambda x: ''.join([{'A':'T','C':'G','G':'C','T':'A'}[B] for B in x][::-1])
	samplesheet_artifact = Artifact(lims, id=aid)

	sample_sheet_id = samplesheet_artifact.files[0].id
	header = ''
	for line in lims.get_file_contents(id=sample_sheet_id).rstrip().split('\n'):
		if line.startswith('Sample_ID'):
			header = line.rstrip().split(',')
			output_file.write('{line}\n'.format(line=line))

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

def version1ToVersion2(lims, step_uri, aid, output_file):
	samplesheet_artifact = Artifact(lims, id=aid)
	sample_sheet_id = samplesheet_artifact.files[0].id

	header = None
	v2_data = {
		'RunName' : None,
		'InstrumentPlatform' : 'NextSeq1k2k',
		'InstrumentType' : 'NextSeq2000',
		'Read1Cycles' : None,
		'Read2Cycles' : None,
		'Index1Cycles' : None,
		'Index2Cycles' :  None,
		'SoftwareVersion' : '3.5.8',
		'AdapterRead1' : 'AGATCGGAAGAGCACACGTCTGAACTCCAGTCA',
		'AdapterRead2' : 'AGATCGGAAGAGCGTCGTGTAGGGAAAGAGTGT',
		'samples' : []

	}
	for line in lims.get_file_contents(id=sample_sheet_id).rstrip().split('\n'):
		if line.startswith('Sample_ID'):
			header = line.rstrip().split(',')
			continue

		if header:
			data = line.rstrip().split(',')
			sample = []
			sample.append( data[ header.index('Sample_Name') ] )

			if 'index' in header:
				sample.append( data[ header.index('index') ] )
				v2_data['Index1Cycles'] = len( data[ header.index('index') ] )
			if 'index2' in header:
				sample.append( data[ header.index('index2') ] )
				v2_data['Index2Cycles'] = len( data[ header.index('index2') ] )

			v2_data['samples'].append(",".join(sample))

		elif line.startswith('Experiment Name'):
			data = line.rstrip().split(',')

			v2_data['RunName'] = data[1]
		elif line[0].isdigit() and not header:
			line = line.rstrip()
			if not v2_data['Read1Cycles']:
				v2_data['Read1Cycles'] = int(line)
			elif not v2_data['Read2Cycles']:
				v2_data['Read2Cycles'] = int(line)


	output_file.write(renderTemplate('SampleSheetv2_template.csv', v2_data))
def run(lims, step_uri, aid, output_file, mode):
	"""Run the reverse complement function"""
	if mode == 'rev':
		reverseComplement(lims, step_uri, aid, output_file)
	elif mode == 'v1tov2':
		version1ToVersion2(lims, step_uri, aid, output_file)
