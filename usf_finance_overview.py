"""report_projects.py

Generate project report table used for (financial) administration

"""
import codecs
import sys

from genologics.lims import *
# Login parameters for connecting to a LIMS instance.
from genologics.config import BASEURI, USERNAME, PASSWORD

### Functions
def get_udf(object, udf_name):
    """Try to get udf and return value"""
    try:
        udf_value = object.udf[udf_name]
    except:
        udf_value = None
    return udf_value

def get_unique_udf(objects, udf_name):
    """From a list of objects (e.g. samples) get certain udf.
    Return: "None", udf_value if unique or "multiple"
    """
    udf_set = set([])
    for object in objects:
        udf_value = get_udf(object, udf_name)
        if udf_value:
            udf_set.add(udf_value)

    if len(udf_set) == 1:
        return udf_set.pop()
    elif len(udf_set) > 2:
        return "multiple"
    else:
        return None

## Writer for utf8 strings
UTF8Writer = codecs.getwriter('utf8')
sys.stdout = UTF8Writer(sys.stdout)

# Create the LIMS interface instance, and check the connection and version.
lims = Lims(BASEURI, USERNAME, PASSWORD)
lims.check_version()

# Get the list of all projects.
projects = lims.get_projects()

#Print table header
print u"id\tname\t\
open_date\tclosed_date\t\
contact name\tcontact email\t\
platform\tsequencing_runtype\trequested_analysis\tsample type\tlibrary prep kit\tfragment size (bp)\treference genome\tprevious run\tprocesses\treagents\t\
account\tproject budget number\taccount budget numbers\tbilling address institution\tbilling address department\tbilling address street\tbilling address postal code\tbilling address city\tbilling address state\tbilling address country"

#Get project and samples
for project in projects:
    project_samples = lims.get_samples(projectlimsid = project.id)
    project_samples = lims.get_batch(project_samples)

    #Parse special items
    account_budget_numbers = get_udf(project.researcher.lab, 'BudgetNrs')
    if account_budget_numbers:
        account_budget_numbers = ','.join(account_budget_numbers.split('\n'))

    #Get processes and reagents
    project_processes = lims.get_processes(projectname = project.name)
    process_list = []
    reagent_list = []
    for process in project_processes:
        process_list.append(process.type.name)
        artifacts = process.all_outputs(unique=True, resolve=True)
        for artifact in artifacts:
            reagent_list += artifact.reagent_labels
    process_list = list(set(process_list))
    reagent_list = list(set(reagent_list))

    # Print row per project
    print u"{id}\t{name}\t\
{open_date}\t{closed_date}\t\
{contact_name}\t{contact_email}\t\
{platform}\t{sequencing_runtype}\t{requested_analysis}\t{sample_type}\t{library_prep_kit}\t{fragment_size}\t{ref_genome}\t{previous_run}\t{processes}\t{reagents}\t\
{account}\t{budget_number}\t{account_budget_numbers}\t{institution}\t{department}\t{street}\t{postal_code}\t{city}\t{state}\t{country}".format(
        id = project.id,
        name = project.name,
        open_date = project.open_date,
        closed_date = project.close_date,
        contact_name = project.researcher.name,
        contact_email = project.researcher.email,
        platform = get_unique_udf(project_samples,'Platform'),
        sequencing_runtype = get_unique_udf(project_samples,'Sequencing Runtype'),
        requested_analysis = get_unique_udf(project_samples,'Analysis'),
        sample_type = get_unique_udf(project_samples,'Sample Type'),
        library_prep_kit = get_unique_udf(project_samples,'Library prep kit'),
        fragment_size = get_unique_udf(project_samples,'Fragment Size (bp)'),
        ref_genome = get_unique_udf(project_samples,'Reference Genome'),
        previous_run = get_unique_udf(project_samples,'Previous Samples'),
        account = project.researcher.lab.name,
        budget_number = get_unique_udf(project_samples,'Budget Number'),
        account_budget_numbers = account_budget_numbers,
        institution = project.researcher.lab.billing_address['institution'],
        department = project.researcher.lab.billing_address['department'],
        street = project.researcher.lab.billing_address['street'],
        postal_code = project.researcher.lab.billing_address['postalCode'],
        city = project.researcher.lab.billing_address['city'],
        state = project.researcher.lab.billing_address['state'],
        country = project.researcher.lab.billing_address['country'],
        processes = ','.join(process_list),
        reagents = ','.join(reagent_list)
    )

# Create general run overviews for financial administration.
# Contains:
# - all contact information         x
# - all account information-project x
# - name-project                    x
# - LIMS ID                         x
# - date opened                     x
# - date closed                     x
# - platform                        x
# -sequencing run type              x
# - workflows used
# - requested analysis              x
# - sample type                     x
# - library prep                    x
# - fragment size                   x
# - reagent type
# - reference genome                x
# - previous run                    x
# - etc
