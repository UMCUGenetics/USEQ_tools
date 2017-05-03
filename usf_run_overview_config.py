from genologics.lims import *
# Login parameters for connecting to a LIMS instance.
from genologics.config import BASEURI, USERNAME, PASSWORD


MAPPING_DIRS={
    'NEXTSEQ':['/hpc/cog_bioinf/cuppen/processed_data/nextseq','/hpc/cog_bioinf/ubec/useq/processed_data/nextseq'],
    'HISEQ':['/hpc/cog_bioinf/cuppen/processed_data/hiseq','/hpc/cog_bioinf/ubec/useq/processed_data/hiseq'],
    'MISEQ':['/hpc/cog_bioinf/cuppen/processed_data/miseq','/hpc/cog_bioinf/ubec/useq/processed_data/miseq']
    
}

RAW_DATA_DIRS={
    'NEXTSEQ' : ['/hpc/cog_bioinf/cuppen/raw_data/nextseq_umc03','/hpc/cog_bioinf/cuppen/raw_data/nextseq_umc04','/hpc/cog_bioinf/cuppen/raw_data/nextseq_umc05', '/hpc/cog_bioinf/ubec/useq/raw_data/nextseq_umc03','/hpc/cog_bioinf/ubec/useq/raw_data/nextseq_umc04','/hpc/cog_bioinf/ubec/useq/raw_data/nextseq_umc05'],
    'HISEQ' : ['/hpc/cog_bioinf/cuppen/raw_data/hiseq_umc01',  '/hpc/cog_bioinf/ubec/useq/raw_data/hiseq_umc01'],
    'MISEQ' : ['/hpc/cog_bioinf/raw_data/miseq', '/hpc/cog_bioinf/cuppen/raw_data/miseq_umc01', '/hpc/cog_bioinf/ubec/useq/raw_data/miseq']
}

BACKUP_DIRS={
    'NEXTSEQ' : ['/data/isi/b/bioinf_dna_archive/PROCESSED/nextseq','/data/isi/b/bioinf_dna_archive/cuppen/processed_data/nextseq'],
    'HISEQ' : ['/data/isi/b/bioinf_dna_archive/PROCESSED/hiseq','/data/isi/b/bioinf_dna_archive/cuppen/processed_data/hiseq'],
    'MISEQ' : ['/data/isi/b/bioinf_dna_archive/PROCESSED/miseq','/data/isi/b/bioinf_dna_archive/cuppen/processed_data/miseq']
}

SEQUENCING_PROCESSES={
    'NEXTSEQ' : 'NextSeq Run (NextSeq) 1.0',
    'HISEQ' : 'Illumina Sequencing (Illumina SBS) 5.0',
    'MISEQ' : 'MiSeq Run (MiSeq) 4.0'
}





