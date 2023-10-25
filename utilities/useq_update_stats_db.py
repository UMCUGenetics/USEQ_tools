from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from config import Config
import sys
from pathlib import Path
from genologics.entities import Project
import glob

def createDBSession():


    #Set up portal db connection +
    Base = automap_base()
    ssl_args = {'ssl_ca': Config.SSL_CERT}
    engine = create_engine(Config.PORTAL_DB_URI, connect_args=ssl_args, pool_pre_ping=True, pool_recycle=21600)

    Base.prepare(engine, reflect=True)
    # Run = Base.classes.run
    IlluminaSequencingStats = Base.classes.illumina_sequencing_stats
    # NanoporeSequencingStats = Base.classes.nanopore_sequencing_stats
    session = Session(engine)

    return (session,IlluminaSequencingStats)


def parseSummaryStats(dir):
    run_name = dir.name
    summary_stats = Path(f'{dir}/{run_name}_summary.csv')
    if not summary_stats.is_file():
        print(f'Warning : Could not find {summary_stats} file.')
        return None

    stats = {
        'yield_r1' : 0,
        'yield_r2' : 0,
    }



    with open(summary_stats, 'r') as sumcsv:
        lines = sumcsv.readlines()
        line_nr = 0
        while line_nr < len(lines):
            # print(line_nr)
            line = lines[line_nr].rstrip()
            if not line: line_nr+=1;continue

            if line.startswith('Read') and len(line) == 6:
                read_nr = 2 if stats["yield_r1"] else 1
                total_yield = []

                #Parse stat block for read nr
                sub_counter = 0
                for sub_line in lines[line_nr+2:]:
                    cols = sub_line.split(",")
                    sub_counter += 1
                    # print(cols)
                    if cols[0].rstrip().isdigit():
                        if '-' in cols[1]:

                            total_yield.append(float( cols[11].rstrip() ))
                    else:
                        sub_counter -= 1
                        break

                line_nr += sub_counter
                #
                stats[f"yield_r{read_nr}"] = round (float(sum(total_yield) ),2)

            else:
                line_nr +=1

    return stats

def updateStats(lims):
    # projects = lims.get_projects()

    run_dirs = {}
    p = Path(Config.HPC_STATS_DIR)

    session,IlluminaSequencingStats = createDBSession()


    for d in p.glob('*/*'):

        flowcell = d.name.split("_")[-1].split("-")[-1]

        run_dirs[flowcell] = d
        run_dirs[flowcell[1:]] = d


    for flowcell in run_dirs:
        stats = session.query(IlluminaSequencingStats).filter_by(flowcell_id=flowcell).first()
        if stats:

            yield_stats = parseSummaryStats( run_dirs[flowcell] )
            if yield_stats:
                stats.yield_r1 = yield_stats['yield_r1']
                stats.yield_r2 = yield_stats['yield_r2']
                session.commit()
                print( f"Updated {flowcell}")
def run(lims):
    updateStats(lims)
