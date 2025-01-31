from jinja2 import Environment, FileSystemLoader
import yaml
import os
import sys

def validate_file_path(file_path: str) -> bool:
    """Validate if the file path has safe extension for Airflow environment"""
    _, ext = os.path.splitext(file_path)
    return ext.lower() in ['.yml', '.yaml', '.jinja2']

def create_config_file(template_file_path, config_path, project_name, vendor) -> str:
    # validate file paths before processing
    if not validate_file_path(config_path):
        raise ValueError(f"Invalid file path or extension: {config_path}")
    
    template_file = os.path.join(template_file_path, "template.jinja2")
    if not validate_file_path(template_file):
        raise ValueError(f"Invalid template file path or extension: {template_file}")

    # get the list of markets from the yaml file
    with open(f"{config_path}", "r") as configfile:
        config_data = yaml.safe_load(configfile)
        markets = config_data[project_name][0]
        market_string = []
        for x in markets:
            market_string.append(x['market_name']+'.')
            
    # get the exclusion list and add to market
    with open(template_file, "r") as configfile:
        base_config = yaml.safe_load(configfile)
        config = {**base_config[project_name][0]}
        if config[f"{vendor}_exclude_list"] != "dummy":
            market_string.append(config[f"{vendor}_exclude_list"])
    
    return ",".join(market_string)

if __name__ == "__main__":
    root_path = sys.argv[1]  # This will be the Airflow DAGs folder path
    project = sys.argv[2]
    
    # derive market mapping yaml files path using root_path
    nokia_market_mapping = os.path.join(root_path,
                           "digital_twin_ran_inventory_nokia/dag/dynamic_dag/digital_twin_ran_inventory_nokia_market_tech_mapping.yml")
    samsung_market_mapping = os.path.join(root_path,
                             "digital_twin_ran_inventory_samsung/dag/dynamic_dag/digital_twin_ran_inventory_samsung_market_tech_mapping.yml")
    ericsson_market_mapping = os.path.join(root_path,
                               "digital_twin_ran_inventory_ericsson/dag/dynamic_dag/digital_twin_ran_inventory_ericsson_market_tech_mapping.yml")
    
    # derive the new market alert yaml file path
    out_file_path = os.path.join(root_path, "digital_twin_ran_inventory_new_market_alert/dag/config")
    template_path = os.path.join(root_path, "digital_twin_ran_inventory_new_market_alert/dag/dynamic_config")
    
    market = {'nokia_market_string': create_config_file(template_path, nokia_market_mapping, project, "nokia"),
              'samsung_market_string': create_config_file(template_path, samsung_market_mapping, project, "samsung"),
              'ericsson_market_string': create_config_file(template_path, ericsson_market_mapping, project, "ericsson"),
              }
    
    print(market.items())
    env = Environment(loader=FileSystemLoader(template_path))
    template = env.get_template('template.jinja2')
    
    # render template and create new file replacing the placeholder
    os.makedirs(out_file_path, exist_ok=True)  # Ensure directory exists
    with open(os.path.join(out_file_path, "base_config_new_market_alert.yml"), 'w+') as f:
        f.write(template.render(market))
