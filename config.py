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
    PORTAL_URL=os.environ.get('PORTAL_URL')
    PORTAL_API_KEY=os.environ.get('PORTAL_API_KEY')
    # PORTAL_DB_USER=os.environ.get('PORTAL_DB_USER')
    # PORTAL_DB_PW=os.environ.get('PORTAL_DB_PW')
    PORTAL_DB_URI=os.environ.get('PORTAL_DB_URI')
    PORTAL_URL=os.environ.get('PORTAL_URL')
    PORTAL_API_KEY=os.environ.get('PORTAL_API_KEY')

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
    #CONV_BCLCONVERT=os.path.join(CONV_SCRIPT_DIR,'bcl-convert-3.10.5-2/usr/bin')
    CONV_BCLCONVERT=os.path.join(CONV_SCRIPT_DIR,'bcl-convert-4.3.6-2/usr/bin')
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
    SMS_SERVER = os.environ.get('SMS_SERVER')

    SSL_CERT = '/etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt'

    ##LIMS SETTINGS##
    PROJECT_TYPES = {'Sequencing': 'USF - Sequencing', 'SNP Fingerprinting':'USF - SNP genotyping'}


    # 'ISOLATION' : {'uri':f'{LIMS_URI}/api/v2/configuration/workflows/1453/stages/5499'},
    # 'LIBPREP-DNA-NANO' : {'uri':f'{LIMS_URI}/api/v2/configuration/workflows/1453/stages/5500'},
    # 'LIBPREP-RNA-POLYA' : {'uri':f'{LIMS_URI}/api/v2/configuration/workflows/1453/stages/5503'},
    # 'LIBPREP-RNA-RIBOZERO' : {'uri':f'{LIMS_URI}/api/v2/configuration/workflows/1453/stages/5506'},

    # 'LIBPREP-ONT-DNA' : {'uri':f'{LIMS_URI}/api/v2/configuration/workflows/1453/stages/5509'},
    # 'LIBPREP-ONT-RNA' : {'uri':f'{LIMS_URI}/api/v2/configuration/workflows/1453/stages/5512'},
    # 'LIBPREP-CHROMIUM-RNA' : {'uri':f'{LIMS_URI}/api/v2/configuration/workflows/1454/stages/5538'},
    # 'POOLING' : {'uri': f'{LIMS_URI}/api/v2/configuration/workflows/1453/stages/5515'},
    # 'BILLING' : {'uri': f'{LIMS_URI}/api/v2/configuration/workflows/1453/stages/5535'},



    WORKFLOW_STEPS = {
        'SEQUENCING' : {
            'steps' : {
                'ISOLATION' : {
                    'names':[
                        'USEQ - Isolation', #old
                        'USEQ - Isolation v2'
                    ],
                    'stage_nrs': {
                        'USEQ - Isolation' : '1602:5895' #PRODUCTION
                        # 'USEQ - Isolation' : '2002:10202' #TEST
                    }
                },
                'LIBPREP' : {
                    'names':[
                        'USEQ - Chromium X Run',
                        'USEQ - Post LibPrep QC',
                        'USEQ - Bioanalyzer QC DNA', #old
                        'USEQ - Qubit QC', #old
                        'USEQ - Adenylate ends & Ligate Adapters', #old
                        'USEQ - Chromium iX Run v1.0' #old-ish
                    ],
                    'stage_nrs' : {

                        ##PRODUCTION##
                        'Truseq DNA nano' : '1602:5896',
                        'Truseq RNA stranded polyA' : '1602:5899',
                        'Truseq RNA stranded ribo-zero' : '1152:4702', #old
                        # "Chromium iX Single Cell 3'RNA" : '1903:10082', #not in new workflow
                        'USEQ - LIBPREP-ONT-DNA' : '1602:5905',
                        'USEQ - LIBPREP-ONT-RNA' : '1602:5908',
                        'USEQ - LIBPREP-FINGERPRINTING' : '1602:5902',
                        ##TEST SERVER##
                        # 'Truseq DNA nano' : '2002:10203',
                        # 'Truseq RNA stranded polyA' : '2002:10206',
                        # 'Truseq RNA stranded ribo-zero' : '2002:10209',
                        # "Chromium iX Single Cell 3'RNA" : '2053:10247', #ON TEST SERVER
                        # 'USEQ - LIBPREP-ONT-DNA' : '2002:10218',
                        # 'USEQ - LIBPREP-ONT-RNA' : '2002:10238',

                    }
                },
                'POOLING' : {
                    'names':['USEQ - Library Pooling'],
                    'stage_nrs' :{

                        'USEQ - Library Pooling' : '1602:5913',#PRODUCTION
                        # 'USEQ - Library Pooling' : '2002:10218',#TEST

                    }
                },
                'POOL QC' : {
                    'names':['USEQ - Aggregate QC (Library Pooling)'],
                    'stage_nrs' : {
                        'USEQ - Pool QC' : '1602:5914',#PRODUCTION
                        # 'USEQ - Pool QC' : '2002:10219',#TEST
                    }
                },
                'ILLUMINA SEQUENCING' : {
                    'names':[
                        'USEQ - MiSeq Run', #old
                        'USEQ - NextSeq Run', #old
                        'USEQ - Automated NovaSeq Run v2', #old
                        'AUTOMATED - NovaSeq Run (NovaSeq 6000 v3.1)',#old
                        'USEQ - iSeq Run',
                        'USEQ - NextSeq2000 Run',
                        'USEQ - NovaSeq X Run'
                    ],
                    'stage_nrs' : {
                        # 'Illumina NextSeq' : '1152:4713',
                        # 'Illumina NextSeq500' : '1152:4713',
                        'Illumina NextSeq2000' : '1602:5916',
                        'NextSeq2000': '1602:5916',
                        # 'Illumina MiSeq' : '1152:4719',
                        # 'Illumina NovaSeq' : '1152:4722',
                        # 'Illumina NovaSeq 6000' : '1152:4722',
                        'Illumina NovaSeq X' : '1602:5922',
                        'NovaSeq X' : '1602:5922',
                        'iSeq 100' : '1602:5919',
                        'Illumina iSeq' : '1602:5919',
                        'Illumina iSeq 100' : '1602:5919'


                    }
                },
                'NANOPORE SEQUENCING' :{
                    'names':[
                        'USEQ - Nanopore Run', #old
                        'USEQ - Nanopore Run v2'
                    ],
                    'stage_nrs': {
                        'Oxford Nanopore' : '1602:5915',
                    }

                },
                'POST SEQUENCING' : {
                    'names':['USEQ - BCL to FastQ','USEQ - Process Raw Data','USEQ - Analysis'],
                    'stage_nrs' : {
                        'USEQ - Post Sequencing' : '1602:5925',
                        'USEQ - Analysis' : '1602:5926',
                        'USEQ - Ready for billing' : '1602:5927',

                    }
                }
            }

        },

        'FINGERPRINTING' :{
            'steps' :{
                'FINGERPRINTING' : {
                    'names':['USEQ - Fingerprinting'],
                    'stage_nrs' :{
                        'USEQ - Fingerprinting' : '652:2054',#old
                    }
                }
            }

        }
    }



    ###Will be integrated in workflow steps###
    RUN_PROCESSES=[
        'USEQ - NextSeq Run',
        'USEQ - MiSeq Run',
        'USEQ - HiSeq Run',
        'USEQ - iSeq Run',
        'USEQ - Nanopore Run v2' ,
        'USEQ - Nanopore Run',
        'USEQ - NextSeq2000 Run',
        'AUTOMATED - NovaSeq Run (NovaSeq 6000 v3.1)',
        'USEQ - NovaSeq X Run',

    ]
    LOAD_PROCESSES = [
        'USEQ - Denature, Dilute and Load (NovaSeq) v2',
        'USEQ - Denature, Dilute and Load (NextSeq2000)',
        'USEQ - Denature, Dilute and Load (NovaSeq)',
    ]
    ISOLATION_PROCESSES=['USEQ - Isolation','USEQ - Isolation v2']
    LIBPREP_PROCESSES=['USEQ - Adenylate ends & Ligate Adapters','USEQ - LibPrep Illumina','USEQ - LibPrep Nanopore','USEQ - Chromium iX Cell Suspension & QC']
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
    NIMAGEN_BARCODES = {
        'RPU0097' : 'RPU0097 (GGATATACGG-AAGTTCGTAA)','RPU0098' : 'RPU0098 (CAACGGTCTT-ACTAATCCAG)','RPU0099' : 'RPU0099 (TCAGATTCTT-TGCCGCAGAG)','RPU0100' : 'RPU0100 (ATGCGATTGA-TTAGAGCTGA)',
        'RPU0101' : 'RPU0101 (GCCGTAATCG-TGGCGTTGGC)','RPU0102' : 'RPU0102 (CGAGTTCGCC-GAATTCTCCA)','RPU0103' : 'RPU0103 (CCGGCTGAGC-ATCGAATCTG)','RPU0104' : 'RPU0104 (TGAATCCGGA-TATAGGTATG)',
        'RPU0105' : 'RPU0105 (CTAAGAGTTA-TAAGTAAGTA)','RPU0106' : 'RPU0106 (GCCAAGGCAA-AAGATCAGTT)','RPU0107' : 'RPU0107 (AATTGAGTTA-TACCTTAGCT)','RPU0108' : 'RPU0108 (TATATGACGT-TATTCCTTGC)',
        'RPU0109' : 'RPU0109 (GTTAGGAGCG-ACCAGTTCAG)','RPU0110' : 'RPU0110 (ATGCTAACCT-AAGGATCCAA)','RPU0111' : 'RPU0111 (TGCAGAGATG-TGGTATTATT)','RPU0112' : 'RPU0112 (TTATTAAGAA-CATCTATTCG)',
        'RPU0113' : 'RPU0113 (TCGTTCATTA-TAATGGTAGA)','RPU0114' : 'RPU0114 (ATTCAATTAA-TAAGGCTGGT)','RPU0115' : 'RPU0115 (CTGCGGACGT-GTTATATGCA)','RPU0116' : 'RPU0116 (AAGAAGTCTA-GTCGACCATT)',
        'RPU0117' : 'RPU0117 (TTAACTCATA-CTGCAGATCC)','RPU0118' : 'RPU0118 (AGAATTAGCA-CGGTCCGCGG)','RPU0119' : 'RPU0119 (AACCTATAGT-TTCTCTACGA)','RPU0120' : 'RPU0120 (TCCTGGTCAA-AAGCTATGCC)',
        'RPU0121' : 'RPU0121 (AGTCATGGAT-GCTGGTTATG)','RPU0122' : 'RPU0122 (TGGAGGCGAA-GAGATTCCAA)','RPU0123' : 'RPU0123 (TTGGCCAGGT-TGAACGGCGT)','RPU0124' : 'RPU0124 (CCTCTAGGAC-CGGATCTCCG)',
        'RPU0125' : 'RPU0125 (TAAGGAACGG-AGACCGCAAG)','RPU0126' : 'RPU0126 (CCGGTCCTAA-TAAGCTCGCC)','RPU0127' : 'RPU0127 (CGTCAGTCAA-TATCTGCCTA)','RPU0128' : 'RPU0128 (TCTTAAGGAC-TCTAGTTCCA)',
        'RPU0129' : 'RPU0129 (GATCATGATA-CATATTGCAT)','RPU0130' : 'RPU0130 (CAAGTAGGAC-CCTGCGTATT)','RPU0131' : 'RPU0131 (CGGCATCTTG-AAGGCCATCA)','RPU0132' : 'RPU0132 (ATTACGTAGG-CAACCTATCA)',
        'RPU0133' : 'RPU0133 (AGACCAGGTT-ATGGCAACTT)','RPU0134' : 'RPU0134 (CCTTGGCTCT-GCGTCCAGGT)','RPU0135' : 'RPU0135 (TAACCAGTTA-CGTAAGCGAA)','RPU0136' : 'RPU0136 (GTATCAATAT-ATTCTGCGGT)',
        'RPU0137' : 'RPU0137 (ATATGGTACC-GTATACGTAA)','RPU0138' : 'RPU0138 (GCTCTTACTT-TACTCCTCGA)','RPU0139' : 'RPU0139 (TGCCTCGCAA-ACTCGGCATA)','RPU0140' : 'RPU0140 (GACCGTTACT-TGAGTTGGAC)',
        'RPU0141' : 'RPU0141 (CCATATGCTC-GGTCTAGGAA)','RPU0142' : 'RPU0142 (CTAACCTACC-TGACGCGACC)','RPU0143' : 'RPU0143 (ACTAGACGTT-GGTACTTCCA)','RPU0144' : 'RPU0144 (GACCTCCTTG-TATGCTTACT)',
        'RPU0145' : 'RPU0145 (GGTCTTCGGT-CGAGCGGCCT)','RPU0146' : 'RPU0146 (GGATGGTATT-GAACGTCAGG)','RPU0147' : 'RPU0147 (CCGTTAGCAA-CGTAGCCGTA)','RPU0148' : 'RPU0148 (CTGGCAGCGG-TTATAGGAGA)',
        'RPU0149' : 'RPU0149 (TGCGAATCGG-TACGCTGGCG)','RPU0150' : 'RPU0150 (TGGACCAAGG-AGGAGTAAGG)','RPU0151' : 'RPU0151 (GCGGTTGGAA-CTACGACTAT)','RPU0152' : 'RPU0152 (TCTGACGAAC-GATCCATAAC)',
        'RPU0153' : 'RPU0153 (TGGATCGTTA-TATCGGCAGT)','RPU0154' : 'RPU0154 (AATGATGCTC-GCTGATAATT)','RPU0155' : 'RPU0155 (GAGTATACCT-ATCCTTGGAG)','RPU0156' : 'RPU0156 (TTCATCGTAA-GTTCGAGGCG)',
        'RPU0157' : 'RPU0157 (TGCCGGTACC-CCTATTACCT)','RPU0158' : 'RPU0158 (GAGGAGAGCT-AGCTGAACGC)','RPU0159' : 'RPU0159 (ACTCTCTAGT-TCGATAAGAA)','RPU0160' : 'RPU0160 (ACGTACTAGG-ATATAGGTTG)',
        'RPU0161' : 'RPU0161 (GCAGAGCCAT-CTAGTTAAGT)','RPU0162' : 'RPU0162 (CATTCTGATG-GTTCGTCTGA)','RPU0163' : 'RPU0163 (CCGAAGGTTA-TGCTGCATCC)','RPU0164' : 'RPU0164 (GCATTAGGCG-GTTCTTATAA)',
        'RPU0165' : 'RPU0165 (AGTTACGCCG-ACCTTCTGGA)','RPU0166' : 'RPU0166 (GCCGGAGCGG-ACGATAGCTG)','RPU0167' : 'RPU0167 (GTCCTATGAA-CGATATATCT)','RPU0168' : 'RPU0168 (ATGCCAGCAA-TAGATCCTAA)',
        'RPU0169' : 'RPU0169 (TAACTGGCTT-TTACTAGTAC)','RPU0170' : 'RPU0170 (TTCTGATTAA-GACTTCTACT)','RPU0171' : 'RPU0171 (AAGCGGCATT-TGCCATGGAA)','RPU0172' : 'RPU0172 (ACGGTAATTG-CTTCTCGACT)',
        'RPU0173' : 'RPU0173 (CGTATTATAC-ACCGGTCTGC)','RPU0174' : 'RPU0174 (TCTGCTATAA-TACTGGACGG)','RPU0175' : 'RPU0175 (ATCGGCTTGA-AATATAGCGG)','RPU0176' : 'RPU0176 (TAATTCCGGT-ATGAAGCTTG)',
        'RPU0177' : 'RPU0177 (GGCTGCGACG-TGGTCTTGAA)','RPU0178' : 'RPU0178 (ACCGGAAGAA-TCAGCGCAAT)','RPU0179' : 'RPU0179 (GGAGGCCTCC-AACCAGCATG)','RPU0180' : 'RPU0180 (TTGATGCCTC-CTTCAGTAGT)',
        'RPU0181' : 'RPU0181 (GTTCCAACTT-TATCGTAGGA)','RPU0182' : 'RPU0182 (CGGATAAGCT-ATAGCATCAA)','RPU0183' : 'RPU0183 (TGCGTCTATA-CTGACTACTT)','RPU0184' : 'RPU0184 (GGTATCATCT-GCTATACTGG)',
        'RPU0185' : 'RPU0185 (ACTTGCTGAT-TCTGGACTCA)','RPU0186' : 'RPU0186 (TTCCAAGTAA-TTAGCGGAAC)','RPU0187' : 'RPU0187 (AGGTTGCAAG-GCGACTCGAT)','RPU0188' : 'RPU0188 (CTATGGCCTA-TCTTAATCAG)',
        'RPU0189' : 'RPU0189 (ATGGAGCTAC-ATGAGTAATA)','RPU0190' : 'RPU0190 (GTTCTCTCCT-CGCCTACCAA)','RPU0191' : 'RPU0191 (TTATATTGAA-GACTCGAGGA)','RPU0192' : 'RPU0192 (TTGGTCGAAT-ACTCCAGATT)'
    }
