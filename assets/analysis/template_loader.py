import jinja2


class TemplateLoader:

    def __init__(self, templates_dir):
        self.template_loader = jinja2.Environment(loader=jinja2.FileSystemLoader(templates_dir))

    def load_from_file(self, file_name, **query_kwargs):
        return self.template_loader.get_template(file_name).render(**query_kwargs)
