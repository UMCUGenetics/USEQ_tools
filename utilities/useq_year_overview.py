from config import Config
import pprint


def getYearOverview(lims, year):
    ovw = {

    }

    projects = lims.get_projects()
    for project in projects:

        if not project.close_date: continue
        if year and not project.close_date.startswith(year) : continue

        if not 'Application' in project.udf : continue #Most projects in 2015 don't have an application yet

        if not project.udf['Application'] in Config.PROJECT_TYPES.values(): continue

        application = project.udf['Application']
        samples = lims.get_samples(projectlimsid=project.id)
        nr_samples = len(samples)
        if nr_samples == 0 : continue

        project_processes = lims.get_processes(type=['USEQ - Ready for billing','Ready for billing' ],projectname=project.name)
        if not project_processes : continue
        # print(project.id)

        # if 'Sequencing' in application:
        billing_year = project.close_date.split("-")[0]
        sample_type = samples[0].udf.get('Sample Type', None)
        platform  = samples[0].udf.get('Platform', None)
        run_type = samples[0].udf.get('Sequencing Runtype', None)
        if not platform and 'SNP' in application:
            platform = 'SNP Fingerprinting'
        elif not platform:
            print(project.id)

        if billing_year not in ovw:
            ovw[billing_year] = {}

        if platform not in ovw[billing_year]:
            ovw[billing_year][platform] = { 'run_types' : {} }

        if run_type not in ovw[billing_year][platform]['run_types']:
            ovw[billing_year][platform]['run_types'][run_type] = { 'sample_types' : {} }

        if sample_type not in ovw[billing_year][platform]['run_types'][run_type]['sample_types']:
            ovw[billing_year][platform]['run_types'][run_type]['sample_types'][sample_type] = {
                'runs' : 0,
                'samples' : 0
            }


        ovw[billing_year][platform]['run_types'][run_type]['sample_types'][sample_type]['runs'] +=1
        ovw[billing_year][platform]['run_types'][run_type]['sample_types'][sample_type]['samples'] += nr_samples

    return ovw


def printOverview(ovw, overview_file):
    overview_file.write("Year;Platform;Run type;Sample Type;Runs;Samples\n")
    for year in ovw:
        for platform in ovw[year]:
            for run_type in ovw[year][platform]['run_types']:
                for sample_type in ovw[year][platform]['run_types'][run_type]['sample_types']:
                    runs = ovw[year][platform]['run_types'][run_type]['sample_types'][sample_type]['runs']
                    samples = ovw[year][platform]['run_types'][run_type]['sample_types'][sample_type]['samples']
                    overview_file.write(f"{year};{platform};{run_type};{sample_type};{runs};{samples}\n")






def run(lims, year, overview_file):

    ovw = getYearOverview(lims, year)
    printOverview(ovw, overview_file)
