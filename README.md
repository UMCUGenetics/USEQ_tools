# Clarity Utils
Utility scripts to work with Genologics Clarity LIMS

## Install
```bash
git clone git@github.com:CuppenResearch/clarity_utils.git
cd clarity_utils
virtualenv env
. env/bin/activate
pip install https://github.com/SciLifeLab/genologics/tarball/master
```
## Configuration
Create genologics.conf in the installation directory. In this file the lims url and user credentials should be written.
```
[genologics]
BASEURI=<https://url.lims.nl:443>
USERNAME=<username>
PASSWORD=<password>
[logging]
MAIN_LOG=/path/to/log/file.txt
```

### report_projects.py
```bash
python report_projects.py > projects_table.txt
```
