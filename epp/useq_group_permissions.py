from genologics.entities import Process

def checkUser(lims, step_uri, groups):
	"""Checks if a user trying to execute a LIMS step is part of the specified group(s)"""
	process_id = step_uri.split("/")[-1]
	process = Process(lims, id=process_id)
	user = process.technician
	lab_name = user.lab.name
	groups = groups.split(",")
	if lab_name not in groups:
		print "User %s %s in %s has not been approved to run steps in this protocol." % ( user.first_name, user.last_name, lab_name )
		exit(-1)

def run(lims, step_uri, groups):
	"""Run checkUser function"""
	checkUser(lims, step_uri, groups)
