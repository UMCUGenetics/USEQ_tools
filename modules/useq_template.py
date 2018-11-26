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
    return TEMPLATE_ENVIRONMENT.get_template(template_filename).render(data)
