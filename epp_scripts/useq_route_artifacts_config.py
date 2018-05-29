NEXT_STEPS = {
    'Truseq DNA nano' : 'https://usf-lims.umcutrecht.nl/api/v2/configuration/workflows/602/stages/1931',
    'Truseq DNA amplicon' : 'https://usf-lims.umcutrecht.nl/api/v2/configuration/workflows/602/stages/1931',
    'Truseq RNA stranded ribo-zero' : 'https://usf-lims.umcutrecht.nl/api/v2/configuration/workflows/602/stages/1944',
    'Truseq RNA stranded polyA' : 'https://usf-lims.umcutrecht.nl/api/v2/configuration/workflows/602/stages/1937',
    'Illumina NextSeq' : 'https://usf-lims.umcutrecht.nl/api/v2/configuration/workflows/602/stages/2003',
    'Illumina MiSeq' : 'https://usf-lims.umcutrecht.nl/api/v2/configuration/workflows/602/stages/2006',
    'Illumina HiSeq' : 'https://usf-lims.umcutrecht.nl/api/v2/configuration/workflows/602/stages/2009',
    'USEQ - Library Pooling' : 'https://usf-lims.umcutrecht.nl/api/v2/configuration/workflows/602/stages/2001',
    'USEQ - Post Sequencing' : 'https://usf-lims.umcutrecht.nl/api/v2/configuration/workflows/602/stages/2012',
    'USEQ - Pool QC' : 'https://usf-lims.umcutrecht.nl/api/v2/configuration/workflows/602/stages/2002'

}

STEP_NAMES = {
    'ISOLATION' : ['USEQ - Isolation'],
    'LIBPREP' : ['USEQ - Bioanalyzer QC DNA'],
    'POOLING' : ['USEQ - Library Pooling'],
    'POOL QC' : ['USEQ - Aggregate QC (Library Pooling)'],
    'SEQUENCING' : ['USEQ - MiSeq Run','USEQ - NextSeq Run','USEQ - HiSeq Run']

}
