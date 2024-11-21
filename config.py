import os,json
from dotenv import load_dotenv

basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '.useq-env'))


class Config(object):

    ##DEVELOPMENT SETTINGS##
    DEVMODE=os.environ.get('DEVMODE') or False

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

    MACHINE_ALIASES=['miseq_umc01','nextseq_umc03','nextseq2000_umc01','nextseq2000_umc02','novaseq_umc01','novaseq_umc02','iseq_umc01','novaseq_umc01/MyRun','novaseq_umc02/MyRun']

    ##HPC SERVER SETTINGS##
    HPC_MAIN_DIR = os.environ.get('HPC_MAIN_DIR')
    HPC_ARCHIVE_DIR = os.environ.get('HPC_ARCHIVE_DIR')
    HPC_TMP_DIR = os.path.join(basedir, 'tmp')
    HPC_RAW_ROOT = os.path.join(HPC_MAIN_DIR,'raw_data')
    HPC_STATS_DIR = os.path.join(HPC_RAW_ROOT, 'runstats')
    HPC_TRANSFER_SERVER='hpct04'

    ##CONVERSION SERVER SETTINGS##
    CONV_MAIN_DIR=os.environ.get('CONV_MAIN_DIR')
    CONV_STAGING_DIR=os.path.join(CONV_MAIN_DIR,'staging')
    CONV_SCRIPT_DIR=os.environ.get('CONV_SCRIPT_DIR')
    CONV_INTEROP=os.path.join(CONV_SCRIPT_DIR,'interop/interop-1.2.0-Linux-GNU')
    CONV_BCLCONVERT=os.path.join(CONV_SCRIPT_DIR,'bcl-convert-3.10.5-2/usr/bin')
    CONV_FASTQC=os.path.join(CONV_SCRIPT_DIR,'FastQC-v0.11.9/')


    # HPC_RAW_NANOPORE=[
    #     os.path.join(HPC_RAW_ROOT, 'nanopore')
    # ]


    ##NEXTCLOUD SERVER SETTINGS##
    NEXTCLOUD_HOST = os.environ.get('NEXTCLOUD_HOST')
    NEXTCLOUD_USER = USEQ_USER
    NEXTCLOUD_PW = os.environ.get('NEXTCLOUD_PW')
    NEXTCLOUD_DATA_ROOT = '/data/ncie02/nextcloud/usfuser/files/'
    NEXTCLOUD_WEBDAV_ROOT = 'remote.php/dav/files/usfuser/'
    NEXTCLOUD_RAW_DIR = 'raw_data/'
    NEXTCLOUD_PROCESSED_DIR = 'processed_data/'
    NEXTCLOUD_MANUAL_DIR = 'other_data/'
    NEXTCLOUD_LOG_DIR = 'log/'
    NEXTCLOUD_STORAGE = 1073741824000 #in bytes
    NEXTCLOUD_MAX = 90 #percent


    ##SMS SERVER SETTINGS##
    SMS_SERVER = 'sumak.op.umcutrecht.nl'

    SSL_CERT = '/etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt'

    ##LIMS SETTINGS##
    PROJECT_TYPES = {'Sequencing': 'USF - Sequencing', 'SNP Fingerprinting':'USF - SNP genotyping'}

    WORKFLOW_STEPS = {
        'SEQUENCING' : {
            'steps' : {
                'ISOLATION' : {
                    'names':['USEQ - Isolation', 'USEQ - Isolation v2', 'USEQ - Chromium iX Run'],
                    'stage_nrs': {
                        #'USEQ - Isolation' : '1152:4695' #PRODUCTION
                        'USEQ - Isolation' : '2002:10202' #TEST
                    }
                },
                'LIBPREP' : {
                    'names':['USEQ - Post LibPrep QC','USEQ - Bioanalyzer QC DNA', 'USEQ - Qubit QC','USEQ - Adenylate ends & Ligate Adapters','USEQ - Chromium iX Run v1.0'],
                    'stage_nrs' : {
                        ##PRODUCTION##
                        # 'Truseq DNA nano' : '1152:4696',
                        # 'Truseq RNA stranded polyA' : '1152:4699',
                        # 'Truseq RNA stranded ribo-zero' : '1152:4702',
                        # "Chromium iX Single Cell 3'RNA" : '1903:10082', #ON TEST SERVER
                        # 'USEQ - LIBPREP-ONT-DNA' : '1152:4705',
                        # 'USEQ - LIBPREP-ONT-RNA' : '1152:4708',
                        ##TEST SERVER##
                        'Truseq DNA nano' : '2002:10203',
                        'Truseq RNA stranded polyA' : '2002:10206',
                        'Truseq RNA stranded ribo-zero' : '2002:10209',
                        "Chromium iX Single Cell 3'RNA" : '2053:10247', #ON TEST SERVER
                        'USEQ - LIBPREP-ONT-DNA' : '2002:10218',
                        'USEQ - LIBPREP-ONT-RNA' : '2002:10238',
                    }
                },
                'POOLING' : {
                    'names':['USEQ - Library Pooling'],
                    'stage_nrs' :{
                        # 'USEQ - Library Pooling' : '1152:4711',#PRODUCTION
                        'USEQ - Library Pooling' : '2002:10218',#TEST
                    }
                },
                'POOL QC' : {
                    'names':['USEQ - Aggregate QC (Library Pooling)'],
                    'stage_nrs' : {
                        # 'USEQ - Pool QC' : '1152:4712',#PRODUCTION
                        'USEQ - Pool QC' : '2002:10219',#TEST
                    }
                },
                'ILLUMINA SEQUENCING' : {
                    'names':['USEQ - MiSeq Run','USEQ - NextSeq Run', 'USEQ - Automated NovaSeq Run v2', 'USEQ - iSeq Run', 'USEQ - NextSeq2000 Run','AUTOMATED - NovaSeq Run (NovaSeq 6000 v3.1)'],
                    'stage_nrs' : {

                        'Illumina NextSeq' : '1152:4713',
                        'Illumina NextSeq500' : '1152:4713',
                        'Illumina NextSeq2000' : '1152:4716',
                        'Illumina MiSeq' : '1152:4719',
                        'Illumina NovaSeq' : '1152:4722',
                        'Illumina NovaSeq 6000' : '1152:4722',
                        'Illumina NovaSeq X' : '1152:4722',#TMP FIX FOR NOVASEQ X
                        'Illumina iSeq' : '1152:4725',
                        'Illumina iSeq 100' : '1152:4725'

                    }
                },
                'NANOPORE SEQUENCING' :{
                    'names':['USEQ - Nanopore Run','USEQ - Nanopore Run v2'],
                    'stage_nrs': {
                        'Oxford Nanopore' : '2002:10235',
                    }

                },
                'POST SEQUENCING' : {
                    'names':['USEQ - BCL to FastQ','USEQ - Process Raw Data','USEQ - Analysis'],
                    'stage_nrs' : {
                        'USEQ - Post Sequencing' : '2002:10236',
                        'USEQ - Analysis' : '2002:10237',
                        'USEQ - Ready for billing' : '2002:10238',
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
    RUN_PROCESSES=['USEQ - NextSeq Run','USEQ - MiSeq Run','USEQ - HiSeq Run', 'USEQ - iSeq Run', 'USEQ - Nanopore Run', 'USEQ - Denature, Dilute and Load (NovaSeq)', 'USEQ - NextSeq2000 Run', 'USEQ - Denature, Dilute and Load (NextSeq2000)','AUTOMATED - NovaSeq Run (NovaSeq 6000 v3.1)','USEQ - Denature, Dilute and Load (NovaSeq) v2']
    ISOLATION_PROCESSES=['USEQ - Isolation','USEQ - Isolation v2']
    LIBPREP_PROCESSES=['USEQ - Adenylate ends & Ligate Adapters','USEQ - LibPrep Illumina','USEQ - LibPrep Nanopore']
    ANALYSIS_PROCESSES=['USEQ - Analysis']
    ###

    RUNTYPE_YIELDS={
        "iSeq Control Software" : 4000000, #iSeq
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
        "NextSeq 2000 P3 Flow Cell Cartridge" : 1000000000,
        "NextSeq 2000 P4 Flow Cell Cartridge" : 1800000000,
        "1.5B Sequencing" :  1500000000,
        "10B Sequencing" : 10000000000,
        "25B Sequencing" : 25000000000,
    }

    UMI_BARCODES={k : f"{k}i7-{k}i5" for k in range(1,385)}
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
