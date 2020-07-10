import os
from jinja2 import Environment, FileSystemLoader

PATH = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_PATH = os.path.join("/".join(PATH.split("/")[0:-1]),'resources')
TEMPLATE_ENVIRONMENT = Environment(
    autoescape=False,
    loader=FileSystemLoader(TEMPLATE_PATH),
    trim_blocks=False)

def renderTemplate(template_filename, data):
    """Render Jinja template."""

    def stringsToUnicode(data):
        """Convert strings to utf8 encoding"""
        if isinstance(data, dict):
            for k,v in data.items():
                if isinstance(v, dict):
                    stringsToUnicode(v)
                elif isinstance(v, list):
                    for i in range(len(v)):
                        stringsToUnicode(v[i])
                elif isinstance(v, set):
                    data[k] = ",".join(v)
                elif isinstance(v, str):
                    data[k] = v


    stringsToUnicode(data)

    return TEMPLATE_ENVIRONMENT.get_template(template_filename).render(data)
