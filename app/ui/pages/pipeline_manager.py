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
    AlertDialog,
    Tabs,
    Tab
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
                    arquivo.write('{'
                                  '\n"node":{"class": "extract", "type": "---", "params": {}},'
                                  '\n"node2":{"class": "transform", "type": "---", "params": {}},'
                                  '\n"node3":{"class": "load", "type": "---", "params": {}},'
                                  '\n"flow":"node->node2->node3"'
                                  '\n}')
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

        def open_variables(e):
            bkp_page = self.page.controls.copy()
            var_list, var_buttons = self.instantiate_variables_window(bkp_page)

            content = Column(
                controls=[
                    Container(height=3),
                    var_list,
                    Divider(),
                    var_buttons
                ],
                alignment=MainAxisAlignment.CENTER,
                scroll=ScrollMode.ALWAYS
            )

            tab = Tabs(
                selected_index=0,
                animation_duration=200,
                tabs=[
                    Tab(
                        text="Set Pipeline variables",
                        icon=icons.LIST,
                        content=content,
                    )
                ],
                expand=1,
                scrollable=True
            )

            self.page.controls = [tab]
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

        variables_button = TextButton(
            text="Set Variables",
            icon=icons.FORMAT_LIST_BULLETED,
            on_click=open_variables,
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
        set_colum = Column(
            controls=[create_button, variables_button],
            alignment=MainAxisAlignment.CENTER
        )
        head = Row(
            controls=[set_colum, lrg_blnk_spce, load_pipe_drop],
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

    def instantiate_variables_window(self, bkp_page):
        with open("variables_paths.json") as j_file:
            j_data = json.load(j_file)

        def done(e):
            dct_to_save = {}
            try:
                for item in var_list_col.controls:
                    dct_to_save[item.controls[0].value] = item.controls[1].value

                with open("variables_paths.json", 'w') as j_w_file:
                    json.dump(dct_to_save, j_w_file)

                error_window = AlertDialog(
                    title=Text("Done!")
                )
                self.page.open(error_window)
                self.page.update()

                self.page.controls = bkp_page
                self.page.update()

            except Exception as err:
                error_window = AlertDialog(
                    title=Text("Error"),
                    content=Text(value=str(err))
                )
                self.page.open(error_window)
                self.page.update()

        def cancel(e):
            self.page.controls = bkp_page
            self.page.update()

        def add(e):
            var_list_col.controls.append(
                Row(
                    controls=[
                        TextField(label="New", value=""),
                        TextField(value="")
                    ],
                    alignment=MainAxisAlignment.CENTER
                )
            )
            self.page.update()

        def remove(e):
            var_list_col.controls.pop()
            self.page.update()

        # Elements ################################
        done_button = TextButton(
            text="Done",
            icon=icons.DONE,
            on_click=done
        )

        cancel_button = TextButton(
            text="Cancel",
            icon=icons.CANCEL_OUTLINED,
            on_click=cancel
        )

        add_button = TextButton(
            text="Add",
            icon=icons.ADD,
            on_click=add
        )
        remove_button = TextButton(
            text="Remove",
            icon=icons.REMOVE,
            on_click=remove
        )

        # Containers ##############################

        r_buttons = Row(
            controls=[
                add_button,
                remove_button,
                Container(width=50),
                cancel_button,
                done_button
            ],
            alignment=MainAxisAlignment.CENTER
        )

        var_list_col = Column(alignment=MainAxisAlignment.CENTER)

        for var in j_data:
            var_list_col.controls.append(
                Row(
                    controls=[
                        TextField(label=var, value=var),
                        TextField(value=j_data[var])
                    ],
                    alignment=MainAxisAlignment.CENTER
                )
            )
        r_window = Column(
            controls=[
                var_list_col
            ],
            alignment=MainAxisAlignment.CENTER,
            height=400,
            scroll=ScrollMode.ALWAYS
        )

        return r_window, r_buttons
