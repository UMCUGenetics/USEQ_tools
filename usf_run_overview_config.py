from genologics.lims import *
# Login parameters for connecting to a LIMS instance.
from genologics.config import BASEURI, USERNAME, PASSWORD


MAPPING_DIRS={
    'NEXTSEQ' : ['/hpc/cog_bioinf/ubec/runs/'],
    'HISEQ' : ['/hpc/cog_bioinf/ubec/runs/'],
    'MISEQ' : ['/hpc/cog_bioinf/ubec/runs/']
}

RAW_DATA_DIRS={
    'NEXTSEQ' : ['/hpc/cog_bioinf/raw_data/nextseq_umc03','/hpc/cog_bioinf/raw_data/nextseq_umc04','/hpc/cog_bioinf/raw_data/nextseq_umc05'],
    'HISEQ' : ['/hpc/cog_bioinf/raw_data/hiseq', '/hpc/cog_bioinf/raw_data/hiseq_umc01'],
    'MISEQ' : ['/hpc/cog_bioinf/raw_data/miseq']
}

BACKUP_DIRS={
    'NEXTSEQ' : ['/data/isi/b/bioinf_dna_archive/PROCESSED/nextseq'],
    'HISEQ' : ['/data/isi/b/bioinf_dna_archive/PROCESSED/hiseq'],
    'MISEQ' : ['/data/isi/b/bioinf_dna_archive/PROCESSED/miseq']
}

SEQUENCING_PROCESSES={
    'NEXTSEQ' : 'NextSeq Run (NextSeq) 1.0',
    'HISEQ' : 'Illumina Sequencing (Illumina SBS) 5.0',
    'MISEQ' : 'MiSeq Run (MiSeq) 4.0'
}





