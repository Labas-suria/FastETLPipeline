from handlers import file, config
from orchestrator import flow

PATH_JSON = 'pipeline.json'

if __name__ == '__main__':

    raw_config_data = file.get_json_from_file(file_path=PATH_JSON)
    prep_config_data = config.get_pipeline_nodes(pipe_config_data=raw_config_data)
    data_cache = flow.execute(list_pipe_config=prep_config_data)

    #print(prep_config_data)
    print(data_cache)
