from jinja2 import Environment, FileSystemLoader
import yaml
import os
import sys


def create_config_file(template_file_path, config_path, project_name, vendor) -> str:
    # get the list of markets from the yaml file
    with open(f"{config_path}", "r") as configfile:
        config_data = yaml.safe_load(configfile)
        markets = config_data[project_name][0]
        market_string = []
        for x in markets:
            market_string.append(x['market_name']+'.')

    # get the exclusion list and add to market
    with open(f"{template_file_path}/template.jinja2", "r") as configfile:
        base_config = yaml.safe_load(configfile)
        config = {**base_config[project_name][0]}
        if config[f"{vendor}_exclude_list"] != "dummy":
            market_string.append(config[f"{vendor}_exclude_list"])

    return ",".join(market_string)


if __name__ == "__main__":
    root_path = sys.argv[1]
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
    with open(f"{out_file_path}/base_config_new_market_alert.yml", 'w+') as f:
        f.write(template.render(market))
