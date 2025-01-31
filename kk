from jinja2 import Environment, FileSystemLoader
import yaml
import os
import sys

def validate_file_path(file_path: str) -> bool:
    """Validate if the file path has safe extension"""
    _, ext = os.path.splitext(file_path)
    return ext.lower() in ['.yml', '.yaml', '.jinja2', '.py']

def create_dag_files(config_path, dag_files_path, project):
    # Validate config file path
    if not validate_file_path(config_path):
        raise ValueError(f"Invalid file path or extension: {config_path}")
    
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    template_path = os.path.join(BASE_DIR, 'template.jinja2')
    if not validate_file_path(template_path):
        raise ValueError(f"Invalid template file path or extension: {template_path}")
        
    env = Environment(loader=FileSystemLoader(BASE_DIR))
    template = env.get_template('template.jinja2')
    
    with open(f"{config_path}", "r") as configfile:
        config_data = yaml.safe_load(configfile)
        markets = config_data[project][0]
        market = {}
        # print(markets)
        for x in markets:
            market['market_name'] = x['market_name']
            market['technologies'] = x['technologies']
            market['recon_days'] = x['recon_days']
            market['ds'] = '{{ data_interval_end.strftime("%Y-%m-%d") }}'
            market['ds_nodash'] = '{{ data_interval_end.strftime("%Y%m%d") }}'
            market['schedule'] = x['schedule']
            market['is_exists'] = "{{ task_instance.xcom_pull(task_ids='market_exists_check_task', key='market_exists') }}"
            market['technologies'] = x['technologies']
            # Render template with config values
            output_file = os.path.join(dag_files_path, f"dtwin_ran_invtry_ericsson_{x['market_name']}.py")
            with open(output_file, 'w+') as f:
                f.write(template.render(market))

if __name__ == "__main__":
    config_path = str(sys.argv[1])
    dag_files_path = str(sys.argv[2])
    project = str(sys.argv[3])
    create_dag_files(config_path, dag_files_path, project)
