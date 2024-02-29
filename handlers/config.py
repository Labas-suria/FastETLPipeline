

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
            print(f"{node}:{pipe_config_data[node]['class']}:{pipe_config_data[node]['type']} node add to queue list!")
    except KeyError as e:
        print(f"Pipeline config is invalid: {e} not found in dict.")
        raise e

    return nodes_list


# def classes_instantiator(list_pipe_config: list) -> list:
#     """
#     Gets list with pipiline nodes in order, and returns a list with objects from classes for each node.
#
#     [{"extr_from_txt2": {"class": "extract","type": "txt",...}}] -> extract.txt.TXT obj
#
#     :param list_pipe_config: list with dict nodes sorted by "flow" string in config file.
#
#     :return: list with objects for each node. Ex: [extract.txt.TXT, transform.basics.Basics, load.microsoft.acess.Access]
#     """
#     data_cash = []
#     for node in list_pipe_config:
#         node_name = list(node)[0]
#         node_class = node[node_name]['class']
#         node_type = node[node_name]['type']
#         node_params = node[node_name]['params']
#         match node_class:
#             case 'extract':
#                 match node_type:
#                     case 'txt':
#                         data_cash.append({"extracted": TXT(**node_params).extract()})
#                     case _:
#                         raise Exception(f"The node type '{node_type}' is not supported in extract class.")
#             case 'transform':
#                 pass
#             case _:
#                 raise Exception(f"The node class '{node_class}' is not supported in instantiator.")
#
#     return data_cash
