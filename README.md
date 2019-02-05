
# Clarity Utils
Utility scripts to work with Genologics Clarity LIMS

## Install
```bash
git clone https://github.com/UMCUGenetics/USEQ_tools.git
cd USEQ_tools
virtualenv env
. env/bin/activate
pip install -r requirements.txt
```
## Configuration
Create config.py file

BASEURI=<LIMS URL>
USERNAME=<LIMS USERNAME>
PASSWORD=<LIMS PASSWORD>


MAIN_LOG=<LOG_FILE>
MAIL_SENDER=<EMAIL ADRESS TO SEND MAILS FROM>
MAIL_ADMINS=[
    <EMAIL ADRESSES>

]


RUN_DIR = <WHERE TO FIND RAW DATA>
RUN_PROCESSES=['NextSeq Run (NextSeq) 1.0','USEQ - NextSeq Run', 'MiSeq Run (MiSeq) 4.0', 'USEQ - MiSeq Run','HiSeq Run (HiSeq) 5.0','USEQ - HiSeq Run']
ISOLATION_PROCESSES=['Qiagen genomic tip DNA isolation','QiaSymphony RNA isolation','USEQ - Isolation']
LIBPREP_PROCESSES=['Enrich DNA fragments (TruSeq Nano) 4.0','Enrich DNA fragments (TruSeq Stranded mRNA) 5.0','Enrich DNA fragments (TruSeq Stranded Total RNA) 5.0','USEQ - Enrich DNA fragments']
ANALYSIS_PROCESSES=['USEQ - Analysis']

#stats settings
RUNTYPE_YIELDS={
    "Version3" : 20000000, #MiSeq
    "Version2" : 12000000, #MiSeq
    "NextSeq Mid" : 120000000, #NextSeq
    "NextSeq High" : 350000000, #NextSeq
    "HiSeq rapid" : 250000000, #Hiseq
}

#Nextcloud settings
NEXTCLOUD_HOST = <NEXTCLOUD URL>
NEXTCLOUD_WEBDAV_ROOT = 'remote.php/webdav/'
NEXTCLOUD_RUN_DIR = 'sequencing_runs/'
NEXTCLOUD_USER = <NEXTCLOUD USER>
NEXTCLOUD_PW = <NEXTCLOUD PASSWORD>
NEXTCLOUD_STORAGE = 1073741824000 #in bytes
NEXTCLOUD_MAX = 90 #percent

#ROUTING config
STEP_URIS = {
    'Truseq DNA nano' : 'https://usf-lims-test.op.umcutrecht.nl/api/v2/configuration/workflows/706/stages/3152',
    'Truseq DNA amplicon' : 'https://usf-lims-test.op.umcutrecht.nl/api/v2/configuration/workflows/706/stages/3152',
    'Truseq RNA stranded polyA' : 'https://usf-lims-test.op.umcutrecht.nl/api/v2/configuration/workflows/706/stages/3158',
    'Truseq RNA stranded ribo-zero' : 'https://usf-lims-test.op.umcutrecht.nl/api/v2/configuration/workflows/706/stages/3165',
    'Illumina NextSeq' : 'https://usf-lims-test.op.umcutrecht.nl/api/v2/configuration/workflows/706/stages/3174',
    'Illumina MiSeq' : 'https://usf-lims-test.op.umcutrecht.nl/api/v2/configuration/workflows/706/stages/3177',
    'Illumina NovaSeq' : 'https://usf-lims-test.op.umcutrecht.nl/api/v2/configuration/workflows/706/stages/3180',
    'USEQ - Library Pooling' : 'https://usf-lims-test.op.umcutrecht.nl/api/v2/configuration/workflows/706/stages/3172',
    'USEQ - Post Sequencing' : 'https://usf-lims-test.op.umcutrecht.nl/api/v2/configuration/workflows/706/stages/3183',
    'USEQ - Pool QC' : 'https://usf-lims-test.op.umcutrecht.nl/api/v2/configuration/workflows/706/stages/3173',
    'USEQ - Analysis' : 'https://usf-lims-test.op.umcutrecht.nl/api/v2/configuration/workflows/706/stages/3184',
    'USEQ - Encrypt & Send' : 'https://usf-lims-test.op.umcutrecht.nl/api/v2/configuration/workflows/706/stages/3185'

}

STEP_NAMES = {
    'ISOLATION' : ['USEQ - Isolation'],
    'LIBPREP' : ['USEQ - Bioanalyzer QC DNA'],
    'POOLING' : ['USEQ - Library Pooling'],
    'POOL QC' : ['USEQ - Aggregate QC (Library Pooling)'],
    'SEQUENCING' : ['USEQ - MiSeq Run','USEQ - NextSeq Run', 'USEQ - NovaSeq Run'],
    'POST SEQUENCING' : ['USEQ - BCL to FastQ']

}

COST_DB = 'http://wgs11.op.umcutrecht.nl/useq/useq_getfinance.php?type=all&mode=json'

#RUN MANAGEMENT SETTINGS
DATA_DIR_CONVERSION = <WHERE TO FIND RAW RUN DATA>
DATA_DIR_HPC = <WHERE TO COPY SEQUENCING RUNS>
ARCHIVE_DIR = <WHERE TO ARCHIVE SEQUENCING RUNS>

INTEROP_PATH = <PATH TO INTEROP BINARY>
BCL2FASTQ_PATH= <PATH TO BCL2FASTQ BINARY>
BCL2FASTQ_PROCESSING_THREADS=10
BCL2FASTQ_WRITING_THREADS=4


