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
                    'names':['USEQ - Isolation', 'USEQ - Isolation v2'],
                    'stage_nrs': {
                        'Truseq DNA nano' : '1501:8806',
                        'Truseq RNA stranded polyA' : '1501:8809',
                        'Truseq RNA stranded ribo-zero' : '1501:8812',
                        'USEQ - LIBPREP-ONT-DNA' : '1501:8815',
                        'USEQ - LIBPREP-ONT-RNA' : '1501:8818'
                        # 'USEQ - LIBPREP-ONT-RNA' : 'https://usf-lims.umcutrecht.nl/api/v2/configuration/workflows/1501/stages/3915'
                    }
                },
                'LIBPREP' : {
                    'names':['USEQ - Post LibPrep QC','USEQ - Bioanalyzer QC DNA', 'USEQ - Qubit QC','USEQ - Adenylate ends & Ligate Adapters'],
                    'stage_nrs' : {}
                },
                'POOLING' : {
                    'names':['USEQ - Library Pooling'],
                    'stage_nrs' :{
                        'USEQ - Library Pooling' : '1501:8821',
                    }
                },
                'POOL QC' : {
                    'names':['USEQ - Aggregate QC (Library Pooling)'],
                    'stage_nrs' : {
                        'USEQ - Pool QC' : '1501:8822',
                    }
                },
                'ILLUMINA SEQUENCING' : {
                    'names':['USEQ - MiSeq Run','USEQ - NextSeq Run', 'USEQ - Automated NovaSeq Run v2', 'USEQ - iSeq Run', 'USEQ - NextSeq2000 Run','AUTOMATED - NovaSeq Run (NovaSeq 6000 v3.1)'],
                    'stage_nrs' : {
                        'Illumina NextSeq' : '1501:8823',
                        'Illumina NextSeq500' : '1501:8823',
                        'Illumina NextSeq2000' : '1501:8826',
                        'Illumina MiSeq' : '1501:8829',
                        'Illumina NovaSeq' : '1501:8832',
                        'Illumina NovaSeq 6000' : '1501:8832',
                        'Illumina iSeq' : '1501:8835',
                        'Illumina iSeq 100' : '1501:8835'
                    }
                },
                'NANOPORE SEQUENCING' :{
                    'names':['USEQ - Nanopore Run','USEQ - Nanopore Run v2'],
                    'stage_nrs': {
                        'Oxford Nanopore' : '1501:8838',
                    }

                },
                'POST SEQUENCING' : {
                    'names':['USEQ - BCL to FastQ','USEQ - Process Raw Data','USEQ - Analysis'],
                    'stage_nrs' : {
                        'USEQ - Post Sequencing' : '1501:8839',
                        'USEQ - Analysis' : '1501:8840',
                        'USEQ - Ready for billing' : '1501:8841',
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

    # WORKFLOW_STEPS = {
    #     'SEQUENCING' : {
    #         'steps' : {
    #             'ISOLATION' : {
    #                 'names':['USEQ - Isolation'],
    #                 'stage_nrs': {
    #                     'Truseq DNA nano' : '851:3892',
    #                     'Truseq RNA stranded polyA' : '851:3905',
    #                     'Truseq RNA stranded ribo-zero' : '851:3898',
    #                     'USEQ - LIBPREP-ONT-DNA' : '851:3912',
    #                     'USEQ - LIBPREP-ONT-RNA' : '851:3915'
    #                     # 'USEQ - LIBPREP-ONT-RNA' : 'https://usf-lims.umcutrecht.nl/api/v2/configuration/workflows/851/stages/3915'
    #                 }
    #             },
    #             'LIBPREP' : {
    #                 'names':['USEQ - Bioanalyzer QC DNA', 'USEQ - Qubit QC','USEQ - Adenylate ends & Ligate Adapters'],
    #                 'stage_nrs' : {}
    #             },
    #             'POOLING' : {
    #                 'names':['USEQ - Library Pooling'],
    #                 'stage_nrs' :{
    #                     'USEQ - Library Pooling' : '851:3919',
    #                 }
    #             },
    #             'POOL QC' : {
    #                 'names':['USEQ - Aggregate QC (Library Pooling)'],
    #                 'stage_nrs' : {
    #                     'USEQ - Pool QC' : '851:3920',
    #                 }
    #             },
    #             'ILLUMINA SEQUENCING' : {
    #                 'names':['USEQ - MiSeq Run','USEQ - NextSeq Run', 'USEQ - Automated NovaSeq Run', 'USEQ - iSeq Run', 'USEQ - NextSeq2000 Run'],
    #                 'stage_nrs' : {
    #                     'Illumina NextSeq' : '851:3921',
    #                     'Illumina NextSeq500' : '851:3921',
    #                     'Illumina NextSeq2000' : '851:3924',
    #                     'Illumina MiSeq' : '851:3927',
    #                     'Illumina NovaSeq' : '851:3930',
    #                     'Illumina NovaSeq 6000' : '851:3930',
    #                     'Illumina iSeq' : '851:3933',
    #                     'Illumina iSeq 100' : '851:3933'
    #                 }
    #             },
    #             'NANOPORE SEQUENCING' :{
    #                 'names':['USEQ - Nanopore Run'],
    #                 'stage_nrs': {
    #                     'Oxford Nanopore' : '851:3936',
    #                 }
    #
    #             },
    #             'POST SEQUENCING' : {
    #                 'names':['USEQ - BCL to FastQ','USEQ - Analysis'],
    #                 'stage_nrs' : {
    #                     'USEQ - Post Sequencing' : '851:3937',
    #                     'USEQ - Analysis' : '851:3938',
    #                     'USEQ - Encrypt & Send' : '851:3939',
    #                 }
    #             }
    #         }
    #
    #     },
    #
    #     'FINGERPRINTING' :{
    #         'steps' :{
    #             'FINGERPRINTING' : {
    #                 'names':['USEQ - Fingerprinting'],
    #                 'stage_nrs' :{
    #                     'USEQ - Fingerprinting' : '652:2054',
    #                 }
    #             }
    #         }
    #
    #     }
    # }

    ###Will be integrated in workflow steps###
    # RUN_PROCESSES=['USEQ - NextSeq Run','USEQ - MiSeq Run','USEQ - HiSeq Run', 'USEQ - iSeq Run', 'USEQ - Nanopore Run', 'USEQ - Denature, Dilute and Load (NovaSeq)', 'USEQ - NextSeq2000 Run', 'USEQ - Denature, Dilute and Load (NextSeq2000)']
    # ISOLATION_PROCESSES=['USEQ - Isolation']
    # LIBPREP_PROCESSES=['USEQ - Adenylate ends & Ligate Adapters']
    # ANALYSIS_PROCESSES=['USEQ - Analysis']
    ###

    ###Will be integrated in workflow steps###
    RUN_PROCESSES=['USEQ - NextSeq Run','USEQ - MiSeq Run','USEQ - HiSeq Run', 'USEQ - iSeq Run', 'USEQ - Nanopore Run', 'USEQ - Denature, Dilute and Load (NovaSeq)', 'USEQ - NextSeq2000 Run', 'USEQ - Denature, Dilute and Load (NextSeq2000)','USEQ - Automated NovaSeq Run v2']
    ISOLATION_PROCESSES=['USEQ - Isolation']
    LIBPREP_PROCESSES=['USEQ - Adenylate ends & Ligate Adapters','USEQ - LibPrep Illumina','USEQ - LibPrep Nanopore']
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

    UMI_BARCODES={k : f"{k}i7-{k}i5" for k in range(1,384)}
    ONT_BARCODES={
        'NB01' : 'NB01 (CACAAAGACACCGACAACTTTCTT)','NB02' : 'NB02 (ACAGACGACTACAAACGGAATCGA)','NB03' : 'NB03 (CCTGGTAACTGGGACACAAGACTC)','NB04' : 'NB04 (TAGGGAAACACGATAGAATCCGAA)','NB05' : 'NB05 (AAGGTTACACAAACCCTGGACAAG)','NB06' : 'NB06 (GACTACTTTCTGCCTTTGCGAGAA)','NB07' : 'NB07 (AAGGATTCATTCCCACGGTAACAC)','NB08' : 'NB08 (ACGTAACTTGGTTTGTTCCCTGAA)',
        'NB09' : 'NB09 (AACCAAGACTCGCTGTGCCTAGTT)','NB10' : 'NB10 (GAGAGGACAAAGGTTTCAACGCTT)','NB11' : 'NB11 (TCCATTCCCTCCGATAGATGAAAC)','NB12' : 'NB12 (TCCGATTCTGCTTCTTTCTACCTG)','NB13' : 'NB13 (AGAACGACTTCCATACTCGTGTGA)','NB14' : 'NB14 (AACGAGTCTCTTGGGACCCATAGA)','NB15' : 'NB15 (AGGTCTACCTCGCTAACACCACTG)','NB16' : 'NB16 (CGTCAACTGACAGTGGTTCGTACT)',
        'NB17' : 'NB17 (ACCCTCCAGGAAAGTACCTCTGAT)','NB18' : 'NB18 (CCAAACCCAACAACCTAGATAGGC)','NB19' : 'NB19 (GTTCCTCGTGCAGTGTCAAGAGAT)','NB20' : 'NB20 (TTGCGTCCTGTTACGAGAACTCAT)','NB21' : 'NB21 (GAGCCTCTCATTGTCCGTTCTCTA)','NB22' : 'NB22 (ACCACTGCCATGTATCAAAGTACG)','NB23' : 'NB23 (CTTACTACCCAGTGAACCTCCTCG)','NB24' : 'NB24 (GCATAGTTCTGCATGATGGGTTAG)',
        'NB25' : 'NB25 (GTAAGTTGGGTATGCAACGCAATG)','NB26' : 'NB26 (CATACAGCGACTACGCATTCTCAT)','NB27' : 'NB27 (CGACGGTTAGATTCACCTCTTACA)','NB28' : 'NB28 (TGAAACCTAAGAAGGCACCGTATC)','NB29' : 'NB29 (CTAGACACCTTGGGTTGACAGACC)','NB30' : 'NB30 (TCAGTGAGGATCTACTTCGACCCA)','NB31' : 'NB31 (TGCGTACAGCAATCAGTTACATTG)','NB32' : 'NB32 (CCAGTAGAAGTCCGACAACGTCAT)',
        'NB33' : 'NB33 (CAGACTTGGTACGGTTGGGTAACT)','NB34' : 'NB34 (GGACGAAGAACTCAAGTCAAAGGC)','NB35' : 'NB35 (CTACTTACGAAGCTGAGGGACTGC)','NB36' : 'NB36 (ATGTCCCAGTTAGAGGAGGAAACA)','NB37' : 'NB37 (GCTTGCGATTGATGCTTAGTATCA)','NB38' : 'NB38 (ACCACAGGAGGACGATACAGAGAA)','NB39' : 'NB39 (CCACAGTGTCAACTAGAGCCTCTC)','NB40' : 'NB40 (TAGTTTGGATGACCAAGGATAGCC)',
        'NB41' : 'NB41 (GGAGTTCGTCCAGAGAAGTACACG)','NB42' : 'NB42 (CTACGTGTAAGGCATACCTGCCAG)','NB43' : 'NB43 (CTTTCGTTGTTGACTCGACGGTAG)','NB44' : 'NB44 (AGTAGAAAGGGTTCCTTCCCACTC)','NB45' : 'NB45 (GATCCAACAGAGATGCCTTCAGTG)','NB46' : 'NB46 (GCTGTGTTCCACTTCATTCTCCTG)','NB47' : 'NB47 (GTGCAACTTTCCCACAGGTAGTTC)','NB48' : 'NB48 (CATCTGGAACGTGGTACACCTGTA)',
        'NB49' : 'NB49 (ACTGGTGCAGCTTTGAACATCTAG)','NB50' : 'NB50 (ATGGACTTTGGTAACTTCCTGCGT)','NB51' : 'NB51 (GTTGAATGAGCCTACTGGGTCCTC)','NB52' : 'NB52 (TGAGAGACAAGATTGTTCGTGGAC)','NB53' : 'NB53 (AGATTCAGACCGTCTCATGCAAAG)','NB54' : 'NB54 (CAAGAGCTTTGACTAAGGAGCATG)','NB55' : 'NB55 (TGGAAGATGAGACCCTGATCTACG)','NB56' : 'NB56 (TCACTACTCAACAGGTGGCATGAA)',
        'NB57' : 'NB57 (GCTAGGTCAATCTCCTTCGGAAGT)','NB58' : 'NB58 (CAGGTTACTCCTCCGTGAGTCTGA)','NB59' : 'NB59 (TCAATCAAGAAGGGAAAGCAAGGT)','NB60' : 'NB60 (CATGTTCAACCAAGGCTTCTATGG)','NB61' : 'NB61 (AGAGGGTACTATGTGCCTCAGCAC)','NB62' : 'NB62 (CACCCACACTTACTTCAGGACGTA)','NB63' : 'NB63 (TTCTGAAGTTCCTGGGTCTTGAAC)','NB64' : 'NB64 (GACAGACACCGTTCATCGACTTTC)',
        'NB65' : 'NB65 (TTCTCAGTCTTCCTCCAGACAAGG)','NB66' : 'NB66 (CCGATCCTTGTGGCTTCTAACTTC)','NB67' : 'NB67 (GTTTGTCATACTCGTGTGCTCACC)','NB68' : 'NB68 (GAATCTAAGCAAACACGAAGGTGG)','NB69' : 'NB69 (TACAGTCCGAGCCTCATGTGATCT)','NB70' : 'NB70 (ACCGAGATCCTACGAATGGAGTGT)','NB71' : 'NB71 (CCTGGGAGCATCAGGTAGTAACAG)','NB72' : 'NB72 (TAGCTGACTGTCTTCCATACCGAC)',
        'NB73' : 'NB73 (AAGAAACAGGATGACAGAACCCTC)','NB74' : 'NB74 (TACAAGCATCCCAACACTTCCACT)','NB75' : 'NB75 (GACCATTGTGATGAACCCTGTTGT)','NB76' : 'NB76 (ATGCTTGTTACATCAACCCTGGAC)','NB77' : 'NB77 (CGACCTGTTTCTCAGGGATACAAC)','NB78' : 'NB78 (AACAACCGAACCTTTGAATCAGAA)','NB79' : 'NB79 (TCTCGGAGATAGTTCTCACTGCTG)','NB80' : 'NB80 (CGGATGAACATAGGATAGCGATTC)',
        'NB81' : 'NB81 (CCTCATCTTGTGAAGTTGTTTCGG)','NB82' : 'NB82 (ACGGTATGTCGAGTTCCAGGACTA)','NB83' : 'NB83 (TGGCTTGATCTAGGTAAGGTCGAA)','NB84' : 'NB84 (GTAGTGGACCTAGAACCTGTGCCA)','NB85' : 'NB85 (AACGGAGGAGTTAGTTGGATGATC)','NB86' : 'NB86 (AGGTGATCCCAACAAGCGTAAGTA)','NB87' : 'NB87 (TACATGCTCCTGTTGTTAGGGAGG)','NB88' : 'NB88 (TCTTCTACTACCGATCCGAAGCAG)',
        'NB89' : 'NB89 (ACAGCATCAATGTTTGGCTAGTTG)','NB90' : 'NB90 (GATGTAGAGGGTACGGTTTGAGGC)','NB91' : 'NB91 (GGCTCCATAGGAACTCACGCTACT)','NB92' : 'NB92 (TTGTGAGTGGAAAGATACAGGACC)','NB93' : 'NB93 (AGTTTCCATCACTTCAGACTTGGG)','NB94' : 'NB94 (GATTGTCCTCAAACTGCCACCTAC)','NB95' : 'NB95 (CCTGTCTGGAAGAAGAATGGACTT)','NB96' : 'NB96 (CTGAACGGTCATAGAGTCCACCAT)'
    }
