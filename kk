from jinja2 import Environment, FileSystemLoader
import yaml
import os
import sys
from pathlib import Path

def validate_file_path(file_path: str) -> bool:
    """Validate if the file path is safe and exists"""
    try:
        path = Path(file_path)
        return path.exists() and path.is_file() and path.suffix in ['.yml', '.yaml', '.jinja2']
    except Exception:
        return False

def safe_read_yaml(file_path: str) -> dict:
    """Safely read and parse YAML file"""
    if not validate_file_path(file_path):
        raise ValueError(f"Invalid or unsafe file path: {file_path}")
    
    with open(file_path, "r", encoding='utf-8') as file:
        try:
            return yaml.safe_load(file)
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML file: {e}")

def create_config_file(template_file_path: str, config_path: str, project_name: str, vendor: str) -> str:
    # Get the list of markets from the yaml file
    config_data = safe_read_yaml(config_path)
    if not config_data or project_name not in config_data:
        raise ValueError(f"Invalid project configuration for {project_name}")
    
    markets = config_data[project_name][0]
    market_string = [x['market_name'] + '.' for x in markets]

    # Get the exclusion list and add to market
    template_file = os.path.join(template_file_path, "template.jinja2")
    base_config = safe_read_yaml(template_file)
    
    if not base_config or project_name not in base_config:
        raise ValueError(f"Invalid template configuration for {project_name}")
    
    config = base_config[project_name][0]
    exclude_key = f"{vendor}_exclude_list"
    
    if exclude_key in config and config[exclude_key] != "dummy":
        market_string.append(config[exclude_key])
    
    return ",".join(market_string)

def main():
    if len(sys.argv) < 3:
        print("Usage: script.py <root_path> <project>")
        sys.exit(1)

    root_path = sys.argv[1]
    project = sys.argv[2]

    if not os.path.isdir(root_path):
        raise ValueError("Invalid root path")

    # Define market mapping paths
    vendors = ['nokia', 'samsung', 'ericsson']
    market_mappings = {
        vendor: os.path.join(
            root_path,
            f"digital_twin_ran_inventory_{vendor}/dag/dynamic_dag/digital_twin_ran_inventory_{vendor}_market_tech_mapping.yml"
        ) for vendor in vendors
    }

    # Validate all required paths exist
    out_file_path = os.path.join(root_path, "digital_twin_ran_inventory_new_market_alert/dag/config")
    template_path = os.path.join(root_path, "digital_twin_ran_inventory_new_market_alert/dag/dynamic_config")

    for path in [*market_mappings.values(), out_file_path, template_path]:
        if not os.path.exists(path):
            raise ValueError(f"Required path does not exist: {path}")

    # Create market strings
    market = {
        f'{vendor}_market_string': create_config_file(template_path, market_mappings[vendor], project, vendor)
        for vendor in vendors
    }

    print(market.items())

    # Setup Jinja2 environment and render template
    env = Environment(loader=FileSystemLoader(template_path), autoescape=True)
    template = env.get_template('template.jinja2')

    # Ensure output directory exists
    os.makedirs(out_file_path, exist_ok=True)

    # Write the rendered template
    output_file = os.path.join(out_file_path, "base_config_new_market_alert.yml")
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(template.render(market))

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)
