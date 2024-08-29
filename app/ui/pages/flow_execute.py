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
    Icon,
    Dropdown,
    dropdown,
    ScrollMode,
    Text,
    colors,
    border
)
import os
import json
import threading
from queue import Queue
import logging
from logging import config

from handlers import file, configuration
from orchestrator import flow

import main

MAIN_PATH = os.path.dirname(main.__file__)
config.fileConfig(os.path.join(MAIN_PATH, 'logging.conf'), disable_existing_loggers=False)
logger = logging.getLogger(__name__)


class FlowExecute:

    def __init__(self, page: Page, path_pipe_files: str):
        self.page = page
        self.path_pipe_files = path_pipe_files
        self.run_button = TextButton()

    def build(self) -> Column:

        def run_pipe(e):

            with open("UIpipelog.log", 'w') as f_log:
                f_log.write("")

            log_show.value = ""

            dropdown_update(e)

            pipeline_file_path = os.path.join(self.path_pipe_files, load_pipe_drop.value)
            node_steps_list = Queue()
            try:
                raw_config_data = file.get_json_from_file(file_path=pipeline_file_path)
                prep_config_data = configuration.get_pipeline_nodes(pipe_config_data=raw_config_data)
            except Exception as err:
                logger.error(err)
                log_show.value = get_uipipelog()
                self.page.update()
                raise

            flow_thread = threading.Thread(
                target=flow.execute,
                args=(prep_config_data, node_steps_list,)
            )
            show_thread = threading.Thread(
                target=self.updates_nodes_in_execution,
                args=(row_node_flow, node_steps_list, log_show,)
            )

            flow_thread.start()
            show_thread.start()
            flow_thread.join()
            show_thread.join()

        def dropdown_update(e):
            row_node_flow.controls = []
            log_show.value = ""

            list_nodes = self.get_nodes_from_file_selected(file_name=load_pipe_drop.value).split('->')

            for node in list_nodes:
                row_node_flow.controls.append(
                    Container(
                        content=Text(value=node),
                        border=border.all(1, colors.BLACK),
                        border_radius=100,
                        bgcolor=colors.SURFACE_VARIANT,
                        alignment=alignment.center,
                        height=100
                    )
                )
                row_node_flow.controls.append(Icon(name=icons.ARROW_RIGHT_ALT))

            load_pipe_drop.options = self.get_pipeline_files_op_list()
            row_node_flow.controls.pop(len(row_node_flow.controls) - 1)
            self.page.update()

        lrg_blnk_spce = Container(alignment=alignment.center, expand=True)

        #  Elements #############

        self.run_button = TextButton(
            text="Run Pipeline",
            icon=icons.PLAY_ARROW,
            on_click=run_pipe
        )

        load_pipe_drop = Dropdown(
            label="Select Pipeline",
            hint_text="Choose your Pipeline",
            options=self.get_pipeline_files_op_list(),
            autofocus=True,
            on_change=dropdown_update
        )

        row_node_flow = Row(alignment=MainAxisAlignment.CENTER)

        log_show = Text(value="")

        # Containers #####################
        head = Row(
            controls=[self.run_button, lrg_blnk_spce, load_pipe_drop],
            alignment=MainAxisAlignment.START
        )

        body = Container(
            content=row_node_flow,
            height=300,
            border=border.all(1, colors.BLACK),
            border_radius=3,
            alignment=alignment.center
        )

        log = Container(
            content=log_show,
            border=border.all(1, colors.BLACK),
            border_radius=3,
            alignment=alignment.center_left,
        )

        r_page = Column(
            controls=[
                Container(height=1),
                head,
                Divider(),
                Text('Pipeline:'),
                body,
                Divider(height=1),
                Text('Log:'),
                log
            ],
            alignment=MainAxisAlignment.START,
            scroll=ScrollMode.ALWAYS
        )

        return r_page

    def get_pipeline_files_op_list(self) -> list:

        file_list = os.listdir(self.path_pipe_files)
        r_list = []
        for file_i in file_list:
            r_list.append(dropdown.Option(file_i))

        return r_list

    def get_nodes_from_file_selected(self, file_name: str):
        file_path = os.path.join(self.path_pipe_files, file_name)
        with open(file_path, encoding="utf-8") as j_file:
            j_data = json.load(j_file)
            flow_str = j_data.pop("flow")

        return flow_str

    def updates_nodes_in_execution(self, row_node_flow: Row, execution_queue: Queue, text: Text):

        bkp_run_button = self.page.controls[0].tabs[1].content.controls[1].controls[0]
        self.page.controls[0].tabs[1].content.controls[1].controls[0] = TextButton(
            text="Pipeline is running...",
            icon=icons.RUN_CIRCLE,
            on_click=None
        )
        self.page.update()

        f_node = ""
        while f_node != "end":
            f_node = execution_queue.get()

            for node in row_node_flow.controls:
                if type(node) != Icon:
                    if node.content.value == f_node:
                        node.bgcolor = colors.GREEN
                        self.page.update()

        self.page.controls[0].tabs[1].content.controls[1].controls[0] = bkp_run_button
        text.value = get_uipipelog()
        self.page.update()


def get_uipipelog() -> str:
    r_line = ""
    with open("UIpipelog.log") as f_log:
        for line in f_log.readlines():
            r_line += line + '\n'

    return r_line
