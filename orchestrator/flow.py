from classes.txt import TXT


def execute(list_pipe_config: list):
    """
    Gets list with pipiline nodes in order, and returns a list with objects from classes for each node.

    [{"extr_from_txt2": {"class": "extract","type": "txt",...}}] -> extract.txt.TXT obj

    :param list_pipe_config: list with dict nodes sorted by "flow" string in config file.

    :return: list with objects for each node. Ex: [extract.txt.TXT, transform.basics.Basics, load.microsoft.acess.Access]
    """
    data_cache = []
    for node in list_pipe_config:
        node_name = list(node)[0]
        node_class = node[node_name]['class']
        node_type = node[node_name]['type']
        node_params = node[node_name]['params']
        match node_class:
            case 'extract':
                match node_type:
                    case 'txt':
                        data_cache.append({"extracted": TXT(**node_params).extract()})
                    case _:
                        raise Exception(f"The node type '{node_type}' is not supported in extract class.")
            case 'transform':
                pass
            case _:
                raise Exception(f"The node class '{node_class}' is not supported in instantiator.")

    return data_cache
