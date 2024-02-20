from handlers import file, config

PATH_JSON = 'pipeline.json'

if __name__ == '__main__':

    raw_config_data = file.get_json_from_file(file_path=PATH_JSON)
    prep_config_data = config.get_pipeline_nodes(pipe_config_data=raw_config_data)
    pipe_class_list = config.classes_instantiator(list_pipe_config=prep_config_data)

    print(pipe_class_list[0].extract())
    print(pipe_class_list[1].extract())
