import os
import argparse


def walkLevel(some_dir, level=1):
    some_dir = some_dir.rstrip(os.path.sep)
    assert os.path.isdir(some_dir)
    num_sep = some_dir.count(os.path.sep)
    for root, dirs, files in os.walk(some_dir):
        yield root, dirs, files
        num_sep_this = root.count(os.path.sep)
        if num_sep + level <= num_sep_this:
            del dirs[:]


def archiveRuns(data, archive):

    for root,dirs,files in walkLevel(data, level=1):
        for d in dirs:
            path = os.path.join(root,d)
            if not os.path.isfile(os.path.join(path,'RTAComplete.txt')): continue #Valid run directory
            if not os.path.isfile(os.path.join(path,'ConversionDone.txt')):continue #Conversion is done
            if not os.path.isfile(os.path.join(path,'TransferDone.txt')):continue #Transfer is done
            if os.path.isfile(os.path.join(path,'ArchiveDone.txt')):continue
            if os.path.isfile(os.path.join(path,'ArchiveRunning.txt')):continue

            open(os.path.join(path,'ArchiveRunning.txt'),'a').close()
            print "Archiving run {0} to {1}".format(path, archive)

            try :
                os.system("rsync -rahm --exclude '*fastq.gz' --exclude '*fq.gz' {0} {1}".format(path, archive))

            except Exception as e:
                print "Failed running rsync of {0} to {1} with error {2}".format(path, archive, e)

            os.remove(os.path.join(path,'ArchiveRunning.txt'))
            open(os.path.join(path,'ArchiveDone.txt'),'a').close()
            try :
                for runroot,subdir,runfiles in os.walk(os.path.join(path,'Data/Intensities/BaseCalls')):
                    for file in runfiles:
                        if file.endswith('.fastq.gz') or file.endswith('.fq.gz'):
                            os.remove(os.path.join(runroot,file))
            except Exception as e:
                print "Failed cleaning up FastQ files for run {0} with error {1}".format(path,e)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('-d', '--datadir')
    parser.add_argument('-a', '--archivedir')

    args = parser.parse_args()

    archiveRuns(args.datadir, args.archivedir)
