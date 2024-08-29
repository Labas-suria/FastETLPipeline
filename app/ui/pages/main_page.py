import flet as ft
from app.ui.pages.pipeline_manager import PipelineManager
from app.ui.pages.flow_execute import FlowExecute


class MainPage:
    def __init__(self, page: ft.Page, path_pipe_files: str):
        self.path_pipe_files = path_pipe_files
        self.page = page

    def build(self):
        pipe_page = PipelineManager(page=self.page, path_pipe_files=self.path_pipe_files).build()
        flow_page = FlowExecute(page=self.page, path_pipe_files=self.path_pipe_files).build()

        t = ft.Tabs(
            selected_index=0,
            animation_duration=200,
            tabs=[
                ft.Tab(
                    text="Pipeline Manager",
                    icon=ft.icons.SETTINGS,
                    content=pipe_page,
                ),
                ft.Tab(
                    text="Flow Execute",
                    icon=ft.icons.PLAY_CIRCLE_OUTLINE_SHARP,
                    content=flow_page
                )
            ],
            expand=1,
            scrollable=True
        )
        self.page.title = "FastETLPipeline"
        self.page.add(t)
