from genologics.entities import Project
from config import Config
# import inspect
import sys


def getSampleMeasurements(lims,pid):
    collected_measurements = {'samples' : {}, 'pool' : {}}
    project = Project(lims, id=pid)
    samples = lims.get_samples(projectlimsid=pid)
    # print(samples)

    for sample in samples:
        if sample.name not in collected_measurements['samples']: collected_measurements['samples'][sample.name] = {}

        sample_artifacts = lims.get_artifacts(samplelimsid=sample.id)
        for sample_artifact in sample_artifacts:
            if not sample_artifact.parent_process: continue
            parent_process = sample_artifact.parent_process
            process_name = parent_process.type.name

            all_udfs = dict(sample_artifact.udf.items())
            if not all_udfs: continue

            if process_name in Config.ISOLATION_PROCESSES:
                collected_measurements['samples'][sample.name]['Isolated conc. (ng/ul)'] = all_udfs.get('Concentration Qubit QC (DNA) 5.0 (ng/ul)', 'NA')
                # if 'Concentration Qubit QC (DNA) 5.0 (ng/ul)' in all_udfs:
            if process_name in ['USEQ - Pre LibPrep QC']:
                collected_measurements['samples'][sample.name]['Pre library prep conc. (ng/ul)'] = all_udfs.get('Concentration Qubit QC (DNA) 5.0 (ng/ul)', 'NA')
                collected_measurements['samples'][sample.name]['RIN'] = all_udfs.get('RIN', 'NA')
            if process_name in ['USEQ - Post LibPrep QC']:
                collected_measurements['samples'][sample.name]['Post library prep conc. (ng/ul)'] = all_udfs.get('Concentration Qubit QC (DNA) 5.0 (ng/ul)', 'NA')
            if process_name in ['USEQ - Qubit QC']:
                collected_measurements['pool']['Library conc. (ng/ul)'] = all_udfs.get('Concentration Qubit QC (DNA) 5.0 (ng/ul)', 'NA')
            if process_name in ['USEQ - Bioanalyzer QC DNA'] or process_name in ['USEQ - Bioanalyzer QC RNA']:
                collected_measurements['pool']['Average length (bp)'] = all_udfs.get('Average length (bp)', 'NA')
    calling_function = sys._getframe(1).f_code.co_name

    if calling_function == 'run':
        for sample in collected_measurements['samples']:
            print(sample)
            for m in collected_measurements['samples'][sample]:
                print(f"\t{m} : {collected_measurements['samples'][sample][m]}")
        print('pool')
        for m in collected_measurements['pool']:
            print(f"\t{m} : {collected_measurements['pool'][m]}")
    return collected_measurements

#OUD7768
def run(lims, pid):

    # print(vars(sys.modules[__name__])['__package__'])
    getSampleMeasurements(lims,pid)
