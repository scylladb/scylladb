import os


def update_properties(nodes: list, properties: dict | None):
    for node in nodes:
        properties.setdefault("dc", node.data_center)
        with open(os.path.join(node.get_conf_dir(), "cassandra-rackdc.properties"), "w") as snitch_file:
            for key, value in properties.items():
                snitch_file.write(f"{key}={value}" + os.linesep)
