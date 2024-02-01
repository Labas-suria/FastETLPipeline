

def get_pipeline_nodes(pipe_config_data: dict) -> list:
    """
    Gets pipeline nodes from configure data and return as a dict list sorted by a "flow" string.

        flow : "node_1->node_2->end" ==> [node_1,node_2]

    :param pipe_config_data: dict with pipeline configuration data extracted from pipeline.json.

    :return: list with dict nodes sorted by "flow" string in config file.
    """
    nodes_list = []
    if pipe_config_data is None:
        raise Exception("Pipeline config is invalid: dict is None.")
    if len(pipe_config_data) < 2:
        raise Exception("Pipeline config is invalid: config must have at last 1 node and flow string.")
    try:
        flow_str = pipe_config_data["flow"]
        nodes_order = flow_str.split('->')

        for node in nodes_order:
            if node not in pipe_config_data.keys():
                raise Exception(f"node '{node}' in flow string not exist in Pipeline config.")
            nodes_list.append({node: pipe_config_data[node]})
            print(f"{node} node add to queue list!")
    except KeyError as e:
        print(f"Pipeline config is invalid: {e} not found in dict.")
        raise e

    return nodes_list
