
def getResearchers(lims):

    researchers = lims.get_researchers()

    for researcher in researchers:
        lab = researcher.lab
        line = []
        line.append(u'{}'.format(researcher.id))
        line.append(u'{}'.format(researcher.first_name))
        line.append(u'{}'.format(researcher.last_name))
        line.append(u'{}'.format(researcher.email))
        try:
            line.append(u'{}'.format(researcher.username))
        except:
            line.append(u'{}'.format('NA'))

        try:
            line.append(u'{}'.format(researcher.account_locked))
        except:
            line.append(u'{}'.format('NA'))

        line.append(u'{}'.format(lab.name))
        line.append(u'{}'.format(lab.id))
        try:
            line.append(u'{}'.format(lab.udf['Budget Number']))
        except:
            line.append(u'{}'.format('NA'))


        print ('\t'.join( (v) for v in line))

        # line = ",".join(line)



def run(lims):
    """Gets all researcher meta data"""

    getResearchers(lims)
