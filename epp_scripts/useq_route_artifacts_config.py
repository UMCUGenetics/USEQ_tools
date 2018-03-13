NEXT_STEPS = {
    'Truseq DNA nano' : 'https://usf-lims-test.op.umcutrecht.nl/api/v2/configuration/workflows/515/stages/1397',
    'Truseq RNA stranded ribo-zero' : 'https://usf-lims-test.op.umcutrecht.nl/api/v2/configuration/workflows/515/stages/1406',
    'Truseq RNA stranded polyA' : 'https://usf-lims-test.op.umcutrecht.nl/api/v2/configuration/workflows/515/stages/1401',
    'Illumina NextSeq' : 'https://usf-lims-test.op.umcutrecht.nl/api/v2/configuration/workflows/515/stages/1413',
    'Illumina MiSeq' : 'https://usf-lims-test.op.umcutrecht.nl/api/v2/configuration/workflows/515/stages/1416',
    'Illumina HiSeq' : 'https://usf-lims-test.op.umcutrecht.nl/api/v2/configuration/workflows/515/stages/1419',
    'USEQ - Library Pooling' : 'https://usf-lims-test.op.umcutrecht.nl/api/v2/configuration/workflows/515/stages/1411',
    'USEQ - Post Sequencing' : 'https://usf-lims-test.op.umcutrecht.nl/api/v2/configuration/workflows/515/stages/1422',
    'USEQ - Pool QC' : 'https://usf-lims-test.op.umcutrecht.nl/api/v2/configuration/workflows/515/stages/1412'

}

STEP_NAMES = {
    'ISOLATION' : ['USEQ - Isolation'],
    'LIBPREP' : ['USEQ - Bioanalyzer QC DNA'],
    'POOLING' : ['USEQ - Library Pooling'],
    'POOL QC' : ['USEQ - Aggregate QC (Library Pooling)'],
    'SEQUENCING' : ['USEQ - MiSeq Run','USEQ - NextSeq Run','USEQ - HiSeq Run']

}
