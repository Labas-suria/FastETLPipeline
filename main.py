from handlers import file, config

PATH_JSON = 'pipeline.json'

if __name__ == '__main__':

    data = file.get_json_from_file(file_path=PATH_JSON)

    # for item in data:
    #     print(f"{item}: {data[item]}")

    print(config.get_pipeline_nodes(pipe_config_data=data))
