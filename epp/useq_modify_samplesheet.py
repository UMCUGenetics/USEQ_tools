from genologics.entities import Artifact

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

def run(lims, step_uri, aid, output_file):
	"""Run the reverse complement function"""
	reverseComplement(lims, step_uri, aid, output_file)
