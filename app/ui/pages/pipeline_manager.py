import json

from flet import (
    Container,
    Row,
    Column,
    MainAxisAlignment,
    alignment,
    Divider,
    TextButton,
    Page,
    icons,
    Dropdown,
    dropdown,
    TextField,
    ScrollMode,
    Text,
    AlertDialog
)
import os


class PipelineManager:

    def __init__(self, page: Page, path_pipe_files: str):
        self.page = page
        self.path_pipe_files = path_pipe_files

    def build(self) -> Column:

        def create_pipe(e):

            def handle_close(e):
                self.page.close(create_window)

            def handle_create(e):
                file_path = os.path.join(self.path_pipe_files, txt_field.value+".json")
                with open(file_path, "w") as arquivo:
                    arquivo.write('{\n"node":"empty",\n"node2":"empty",\n"flow":"node->node2"\n}')
                load_pipe_drop.options = self.get_pipeline_files_op_list()
                self.page.update()
                self.page.close(create_window)

            txt_field = TextField(label="Pipeline name")
            create_window = AlertDialog(
                modal=True,
                title=Text("New Pipeline file"),
                actions=[
                    txt_field,
                    Row(
                        controls=[
                            TextButton("Create", on_click=handle_create),
                            TextButton("Cancel", on_click=handle_close)
                        ],
                        alignment=MainAxisAlignment.CENTER
                    )
                ],
                actions_alignment=MainAxisAlignment.END,
            )
            self.page.open(create_window)
            self.page.update()

        def save_pipe(e):
            try:
                to_save = json.loads(node_descr_input.value)
                to_save['flow'] = flow_input.value
                self.save_pipeline_file(file_name=load_pipe_drop.value, content=to_save)
                saved_window = AlertDialog(
                    title=Text("Pipeline Saved!"),
                )
                self.page.open(saved_window)
                load_pipe_drop.options = self.get_pipeline_files_op_list()
                self.page.update()
            except Exception as err:
                error_window = AlertDialog(
                    title=Text("Error"),
                    content=Text(value=str(err))
                )
                self.page.open(error_window)
                self.page.update()

        def delete_pipe(e):
            self.delete_pipeline_file(file_name=load_pipe_drop.value)
            deleted_window = AlertDialog(
                title=Text("Pipeline deleted!"),
            )
            self.page.open(deleted_window)
            load_pipe_drop.options = self.get_pipeline_files_op_list()
            node_descr_input.value = ""
            flow_input.value = ""
            self.page.update()

        def dropdown_update(e):
            node_descr_input.value, flow_input.value = self.get_cont_pipe_file_selected(file_name=load_pipe_drop.value)
            load_pipe_drop.options = self.get_pipeline_files_op_list()
            self.page.update()

        lrg_blnk_spce = Container(alignment=alignment.center, expand=True)

        #  Elements #############

        create_button = TextButton(
            text="Create New Pipeline",
            icon=icons.ADD_BOX_OUTLINED,
            on_click=create_pipe
        )

        save_button = TextButton(
            text="Save",
            icon=icons.SAVE,
            on_click=save_pipe,
        )

        delete_button = TextButton(
            text="Delete",
            icon=icons.DELETE,
            on_click=delete_pipe,
        )

        load_pipe_drop = Dropdown(
            label="Select Pipeline",
            hint_text="Choose your Pipeline",
            options=self.get_pipeline_files_op_list(),
            autofocus=True,
            on_change=dropdown_update
        )

        node_descr_input = TextField(label="Nodes Description", multiline=True, min_lines=20)
        flow_input = TextField(label="Flow String")

        # Containers #####################
        head = Row(
            controls=[create_button, lrg_blnk_spce, load_pipe_drop],
            alignment=MainAxisAlignment.START
        )

        body = Column(
            controls=[
                node_descr_input,
                flow_input
            ],
            alignment=MainAxisAlignment.CENTER
        )
        buttons_row = Row(controls=[save_button, delete_button], alignment=MainAxisAlignment.CENTER)
        r_page = Column(
            controls=[
                Container(height=1),
                head,
                Divider(),
                body,
                Container(content=buttons_row, alignment=alignment.center)
            ],
            alignment=MainAxisAlignment.START,
            scroll=ScrollMode.ALWAYS
        )

        return r_page

    def get_pipeline_files_op_list(self) -> list:

        file_list = os.listdir(self.path_pipe_files)
        r_list = []
        for file in file_list:
            r_list.append(dropdown.Option(file))

        return r_list

    def get_cont_pipe_file_selected(self, file_name: str):
        file_path = os.path.join(self.path_pipe_files, file_name)
        with open(file_path, encoding="utf-8") as j_file:
            j_data = json.load(j_file)
            flow_str = j_data.pop("flow")
            j_data = json.dumps(j_data, indent=6, ensure_ascii=False).encode('utf8')
            j_data = j_data.decode()

        return j_data, flow_str

    def save_pipeline_file(self, file_name: str, content: dict):
        file_path = os.path.join(self.path_pipe_files, file_name)
        with open(file_path, 'w') as j_file:
            json.dump(content, j_file)

    def delete_pipeline_file(self, file_name: str):
        file_path = os.path.join(self.path_pipe_files, file_name)
        os.remove(file_path)
