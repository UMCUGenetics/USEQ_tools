import os,json
from dotenv import load_dotenv

basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '.useq-env'))


class Config(object):

    ##USEQ MAIN USER ACCOUNT##
    USEQ_USER=os.environ.get('USEQ_USER') or 'usfuser'

    ## PORTAL SETTINGS##
    PORTAL_SERVER=os.environ.get('PORTAL_SERVER')
    PORTAL_USER=os.environ.get('PORTAL_USER')
    PORTAL_STORAGE=os.environ.get('PORTAL_STORAGE')
    # PORTAL_DB_USER=os.environ.get('PORTAL_DB_USER')
    # PORTAL_DB_PW=os.environ.get('PORTAL_DB_PW')
    PORTAL_DB_URI=os.environ.get('PORTAL_DB_URI')

    ##LIMS SETTINGS##
    LIMS_URI=os.environ.get('LIMS_URI')
    LIMS_USER=os.environ.get('LIMS_USER')
    LIMS_PW=os.environ.get('LIMS_PW')

    ##MAIL SETTINGS##
    MAIL_HOST=os.environ.get('MAIL_HOST')
    MAIL_SENDER=os.environ.get('MAIL_SENDER')
    MAIL_ADMINS=os.environ.get('MAIL_ADMINS').split(",")

    ##TRELLO##
    TRELLO_ANALYSIS_BOARD='swboymans+dz1sjzuejttnkxhgya6p@boards.trello.com'

    MACHINE_ALIASES=['miseq_umc01','nextseq_umc03','nextseq2000_umc01','nextseq2000_umc02','novaseq_umc01','novaseq_umc02','iseq_umc01']

    ##HPC SERVER SETTINGS##
    HPC_MAIN_DIR = os.environ.get('HPC_MAIN_DIR')
    HPC_ARCHIVE_DIR = os.environ.get('HPC_ARCHIVE_DIR')
    HPC_TMP_DIR = os.path.join(basedir, 'tmp')
    HPC_RAW_ROOT = os.path.join(HPC_MAIN_DIR,'raw_data')
    HPC_STATS_DIR = os.path.join(HPC_RAW_ROOT, 'runstats')
    HPC_TRANSFER_SERVER='hpct02'

    ##CONVERSION SERVER SETTINGS##
    CONV_MAIN_DIR=os.environ.get('CONV_MAIN_DIR')
    CONV_STAGING_DIR=os.path.join(CONV_MAIN_DIR,'staging')
    CONV_SCRIPT_DIR=os.environ.get('CONV_SCRIPT_DIR')
    CONV_INTEROP=os.path.join(CONV_SCRIPT_DIR,'interop/InterOp-1.1.12-Linux-GNU/')
    CONV_BCLCONVERT=os.path.join(CONV_SCRIPT_DIR,'bcl-convert-3.10.5-2/usr/bin')

    # HPC_RAW_NANOPORE=[
    #     os.path.join(HPC_RAW_ROOT, 'nanopore')
    # ]


    ##NEXTCLOUD SERVER SETTINGS##
    NEXTCLOUD_HOST = os.environ.get('NEXTCLOUD_HOST')
    NEXTCLOUD_USER = USEQ_USER
    NEXTCLOUD_PW = os.environ.get('NEXTCLOUD_PW')
    NEXTCLOUD_DATA_ROOT = '/data/ncie01/nextcloud/usfuser/files/'
    NEXTCLOUD_WEBDAV_ROOT = 'remote.php/dav/files/usfuser/'
    NEXTCLOUD_RAW_DIR = 'raw_data/'
    NEXTCLOUD_PROCESSED_DIR = 'processed_data/'
    NEXTCLOUD_MANUAL_DIR = 'other_data/'
    NEXTCLOUD_LOG_DIR = 'log/'
    NEXTCLOUD_STORAGE = 1073741824000 #in bytes
    NEXTCLOUD_MAX = 90 #percent


    ##SMS SERVER SETTINGS##
    SMS_SERVER = 'sumak.op.umcutrecht.nl'


    ##LIMS SETTINGS##
    PROJECT_TYPES = {'Sequencing': 'USF - Sequencing', 'SNP Fingerprinting':'USF - SNP genotyping'}

    WORKFLOW_STEPS = {
        'SEQUENCING' : {
            'steps' : {
                'ISOLATION' : {
                    'names':['USEQ - Isolation'],
                    'stage_nrs': {
                        'Truseq DNA nano' : '851:3892',
                        'Truseq RNA stranded polyA' : '851:3905',
                        'Truseq RNA stranded ribo-zero' : '851:3898',
                        'USEQ - LIBPREP-ONT-DNA' : '851:3912',
                        'USEQ - LIBPREP-ONT-RNA' : '851:3915'
                        # 'USEQ - LIBPREP-ONT-RNA' : 'https://usf-lims.umcutrecht.nl/api/v2/configuration/workflows/851/stages/3915'
                    }
                },
                'LIBPREP' : {
                    'names':['USEQ - Bioanalyzer QC DNA', 'USEQ - Qubit QC','USEQ - Adenylate ends & Ligate Adapters'],
                    'stage_nrs' : {}
                },
                'POOLING' : {
                    'names':['USEQ - Library Pooling'],
                    'stage_nrs' :{
                        'USEQ - Library Pooling' : '851:3919',
                    }
                },
                'POOL QC' : {
                    'names':['USEQ - Aggregate QC (Library Pooling)'],
                    'stage_nrs' : {
                        'USEQ - Pool QC' : '851:3920',
                    }
                },
                'ILLUMINA SEQUENCING' : {
                    'names':['USEQ - MiSeq Run','USEQ - NextSeq Run', 'USEQ - Automated NovaSeq Run', 'USEQ - iSeq Run', 'USEQ - NextSeq2000 Run'],
                    'stage_nrs' : {
                        'Illumina NextSeq' : '851:3921',
                        'Illumina NextSeq500' : '851:3921',
                        'Illumina NextSeq2000' : '851:3924',
                        'Illumina MiSeq' : '851:3927',
                        'Illumina NovaSeq' : '851:3930',
                        'Illumina NovaSeq 6000' : '851:3930',
                        'Illumina iSeq' : '851:3933',
                        'Illumina iSeq 100' : '851:3933'
                    }
                },
                'NANOPORE SEQUENCING' :{
                    'names':['USEQ - Nanopore Run'],
                    'stage_nrs': {
                        'Oxford Nanopore' : '851:3936',
                    }

                },
                'POST SEQUENCING' : {
                    'names':['USEQ - BCL to FastQ','USEQ - Analysis'],
                    'stage_nrs' : {
                        'USEQ - Post Sequencing' : '851:3937',
                        'USEQ - Analysis' : '851:3938',
                        'USEQ - Encrypt & Send' : '851:3939',
                    }
                }
            }

        },

        'FINGERPRINTING' :{
            'steps' :{
                'FINGERPRINTING' : {
                    'names':['USEQ - Fingerprinting'],
                    'stage_nrs' :{
                        'USEQ - Fingerprinting' : '652:2054',
                    }
                }
            }

        }
    }

    ###Will be integrated in workflow steps###
    RUN_PROCESSES=['USEQ - NextSeq Run','USEQ - MiSeq Run','USEQ - HiSeq Run', 'USEQ - iSeq Run', 'USEQ - Nanopore Run', 'USEQ - Denature, Dilute and Load (NovaSeq)', 'USEQ - NextSeq2000 Run', 'USEQ - Denature, Dilute and Load (NextSeq2000)']
    ISOLATION_PROCESSES=['USEQ - Isolation']
    LIBPREP_PROCESSES=['USEQ - Adenylate ends & Ligate Adapters']
    ANALYSIS_PROCESSES=['USEQ - Analysis']
    ###

    RUNTYPE_YIELDS={
        "Version3" : 20000000, #MiSeq
        "Version2" : 12000000, #MiSeq
        "NextSeq Mid" : 120000000, #NextSeq
        "NextSeq High" : 350000000, #NextSeq
        "HiSeq rapid" : 250000000, #Hiseq
        "S1" : 1500000000,
        "S2" : 3500000000,
        "S4" : 9000000000,
        "SP" : 750000000,
        "NextSeq 1000/2000 P1 Flow Cell Cartridge" : 100000000,
        "NextSeq 1000/2000 P2 Flow Cell Cartridge" : 400000000,
        "NextSeq 2000 P3 Flow Cell Cartridge" : 1000000000

    }
