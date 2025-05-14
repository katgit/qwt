from shiny import App, reactive, render, ui
from homepage import homepage_ui, homepage_server
from gpu_job import gpu_job_ui, gpu_job_server
from mpi_job import mpi_job_ui, mpi_job_server
from omp_job import omp_job_ui, omp_job_server
from onep_job import oneP_job_ui, oneP_job_server 
import datetime


# Import additional page logic if needed (e.g., GPU Job, MPI Job)
# from gpu_job import gpu_job_ui, gpu_job_server

app_ui = ui.page_fluid(
    ui.tags.div(
        ui.navset_bar(
            ui.nav_panel("All Jobs"),
            ui.nav_panel("GPU Job"),
            ui.nav_panel("MPI Job"),
            ui.nav_panel("OMP Job"),
            ui.nav_panel("1-p Job"),
            id="selected_navset_bar",
            title="Entry Job Analysis",
        ),
        id="nav-bar-content",
        style="background-color: transparent !important; padding: 10px; height: 75px;"
    ),
    ui.output_ui("page_content")
)

# auto default month & year value
now = datetime.datetime.now()
month_str = now.strftime("%b")  # "Jan", "Feb", etc.
year_str = str(now.year)

selected_year = reactive.Value(year_str)
selected_month = reactive.Value(month_str)

def server(input, output, session):
    current_page = reactive.Value("All Jobs")

    @reactive.effect
    def update_page():
        current_page.set(input.selected_navset_bar())

    # Initialize all server logic once
    homepage_server(input, output, session, selected_year, selected_month)
    gpu_job_server(input, output, session, selected_year, selected_month)
    mpi_job_server(input, output, session, selected_year, selected_month)
    omp_job_server(input, output, session, selected_year, selected_month)
    oneP_job_server(input, output, session, selected_year, selected_month)

    # Only show active page
    @output
    @render.ui
    def page_content():
        active = current_page.get()

        def visible(page_name):
            return "display: block;" if active == page_name else "display: none;"

        return ui.tags.div(
            ui.tags.div(homepage_ui(selected_year, selected_month), id="all-jobs", style=visible("All Jobs")),
            ui.tags.div(gpu_job_ui(selected_year, selected_month), id="gpu-job", style=visible("GPU Job")),
            ui.tags.div(mpi_job_ui(selected_year, selected_month), id="mpi-job", style=visible("MPI Job")),
            ui.tags.div(omp_job_ui(selected_year, selected_month), id="omp-job", style=visible("OMP Job")),
            ui.tags.div(oneP_job_ui(selected_year, selected_month), id="onep-job", style=visible("1-p Job")),
        )


app = App(app_ui, server)



