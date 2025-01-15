import faicons as fa
import pandas as pd
from shiny import ui, render, reactive
from shinywidgets import output_widget, render_plotly
import plotly.express as px
import plotly.graph_objects as go

# ----------------------------------------------------------------
# DATA LOADING & PREP
# ----------------------------------------------------------------

dataset = pd.read_feather("/projectnb/rcs-intern/Jiazheng/accounting/ShinyApp_Data_MPI.feather")

# Ensure 'year' is integer
dataset["year"] = dataset["year"].astype(int)

month_order = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
]
dataset["month"] = pd.Categorical(
    dataset["month"], 
    categories=month_order, 
    ordered=True
)

ICONS = {
    "min": fa.icon_svg("arrow-down"),
    "max": fa.icon_svg("arrow-up"),
    "mean": fa.icon_svg("users"),
    "median": fa.icon_svg("battery-half"),
    "currency-dollar": fa.icon_svg("dollar-sign"),
    "ellipsis": fa.icon_svg("ellipsis"),
    "clock": fa.icon_svg("clock"),
    "speed": fa.icon_svg("gauge"),
    "chart-bar": fa.icon_svg("chart-bar"),
    "calendar": fa.icon_svg("calendar"),
    "comment": fa.icon_svg("comment"),
    "bell": fa.icon_svg("bell"),
    "camera": fa.icon_svg("camera"),
    "heart": fa.icon_svg("heart"),
    "count": fa.icon_svg("list"),
}

# ----------------------------------------------------------------
# UI FOR THE MPI JOB PAGE
# ----------------------------------------------------------------

def mpi_job_ui():
    """
    Builds the UI for the MPI Job page,
    including year checkboxes, summary value boxes, and multiple plots.
    """
    return ui.page_fluid(
        ui.input_checkbox_group(
            "years",
            "Select Year(s)",
            list(range(2013, 2026)),  # 2013-2025
            selected=[2024],
            inline=True
        ),
        ui.layout_columns(
            ui.value_box("Min Waiting Time", ui.output_text("min_waiting_time"), showcase=ICONS["min"]),
            ui.value_box("Max Waiting Time", ui.output_text("max_waiting_time"), showcase=ICONS["max"]),
            ui.value_box("Mean Waiting Time", ui.output_text("mean_waiting_time"), showcase=ICONS["speed"]),
            ui.value_box("Median Waiting Time", ui.output_text("median_waiting_time"), showcase=ICONS["median"]),
            ui.value_box("Number of Jobs", ui.output_text("job_count"), showcase=ICONS["count"]),
            fill=False,
        ),
        ui.layout_columns(
            # ui.card(
            #     ui.card_header("Dataset Data"),
            #     ui.output_data_frame("displayTable"),
            #     full_screen=True
            # ),
            ui.card(
                ui.card_header(
                    "Waiting Time vs Job Type",
                    ui.popover(
                        ICONS["ellipsis"],
                        ui.input_radio_buttons(
                            "scatter_color",
                            None,
                            ["job_type", "none"],
                            inline=True,
                        ),
                        title="Add a color variable",
                        placement="top",
                    ),
                    class_="d-flex justify-content-between align-items-center",
                ),
                output_widget("barplot"),
                full_screen=True
            ),
            ui.card(
                ui.card_header(
                    "Box Plot of Job Waiting Time by Month & Year",
                    class_="d-flex justify-content-between align-items-center"
                ),
                output_widget("job_waiting_time_by_month"),
                full_screen=True
            ),
            ui.card(
                ui.card_header(
                    "Box Plot of Job Waiting Time by CPU Cores",
                    class_="d-flex justify-content-between align-items-center"
                ),
                output_widget("job_waiting_time_by_cpu"),
                full_screen=True
            ),
            col_widths=[6, 6, 6, 6]
        ),
        fillable=True,
    )

# ----------------------------------------------------------------
# SERVER LOGIC
# ----------------------------------------------------------------

def mpi_job_server(input, output, session):
    print("MPI Job server function called")

    # ----------------------------------------------------------------
    # 1) Reactive data filter by selected years (and possibly more in future)
    # ----------------------------------------------------------------
    @reactive.calc
    def dataset_data():
        """
        Filter the dataset based on user inputs (years).
        Returns the subset used throughout the app (table, plots, stats).
        """
        print("dataset_data function called for MPI Job")
        years = list(map(int, input.years()))

        if not years:
            # If nothing is selected, return an empty DataFrame
            return dataset.iloc[0:0]

        # Filter by year
        df_filtered = dataset[dataset["year"].isin(years)]
        # If needed, filter by job_type = "MPI" (some dataset might have multiple job types)
        # df_filtered = df_filtered[df_filtered["job_type"].str.contains("MPI")]

        print(f"Filtered data shape: {df_filtered.shape}")
        return df_filtered

    # ----------------------------------------------------------------
    # 2) TABLE OUTPUT
    # ----------------------------------------------------------------
    # @output
    # @render.data_frame
    # def displayTable():
    #     """
    #     Displays filtered dataset in a table.
    #     - Converts waiting time from seconds -> minutes for readability.
    #     - Renames columns for user-friendly output.
    #     - Strips 'MPI job' prefix from job_type if present.
    #     """
    #     df = dataset_data()
    #     if df.empty:
    #         return pd.DataFrame()

    #     df_disp = df.copy()
    #     # Convert sec -> minutes
    #     df_disp["first_job_waiting_time"] = (df_disp["first_job_waiting_time"] / 60).round(1)
    #     # Remove 'MPI job ' prefix if present
    #     df_disp["job_type"] = df_disp["job_type"].str.replace("MPI job ", "", regex=False)
    #     # Sort by job_number
    #     df_disp.sort_values(by="job_number", ascending=True, inplace=True)

    #     # Rename columns for display
    #     df_disp.rename(
    #         columns={
    #             "job_type": "Queue",
    #             "first_job_waiting_time": "Waiting Time (min)",
    #             "month": "Month",
    #             "job_number": "Job Number",
    #             "year": "Year",
    #             "slots": "CPU Cores",
    #         },
    #         inplace=True
    #     )
    #     return df_disp

    # ----------------------------------------------------------------
    # 3) SUMMARY STATS (MIN, MAX, MEAN, MEDIAN, COUNT)
    # ----------------------------------------------------------------
    @reactive.calc
    def stats():
        """
        Calculate key summary statistics (in minutes) for the filtered data.
        Returns a dict {min, max, mean, median, count}.
        """
        df = dataset_data()
        if df.empty:
            return {"min": None, "max": None, "mean": None, "median": None, "count": 0}

        waiting_sec = df["first_job_waiting_time"]
        return {
            "min": max(waiting_sec.min() / 60.0, 0),
            "max": waiting_sec.max() / 60.0,
            "mean": waiting_sec.mean() / 60.0,
            "median": waiting_sec.median() / 60.0,
            "count": df.shape[0],
        }

    @output
    @render.text
    def min_waiting_time():
        s = stats()
        if s["min"] is None:
            return "No data available"
        return f"{s['min'] / 60:.1f} hours" if s["min"] > 60 else f"{s['min']:.1f} min"

    @output
    @render.text
    def max_waiting_time():
        s = stats()
        if s["max"] is None:
            return "No data available"
        return f"{s['max'] / 60:.1f} hours" if s["max"] > 60 else f"{s['max']:.1f} min"

    @output
    @render.text
    def mean_waiting_time():
        s = stats()
        if s["mean"] is None:
            return "No data available"
        return f"{s['mean'] / 60:.1f} hours" if s["mean"] > 60 else f"{s['mean']:.1f} min"

    @output
    @render.text
    def median_waiting_time():
        s = stats()
        if s["median"] is None:
            return "No data available"
        return f"{s['median'] / 60:.1f} hours" if s["median"] > 60 else f"{s['median']:.1f} min"

    @output
    @render.text
    def job_count():
        return str(stats()["count"])

    # ----------------------------------------------------------------
    # 4) OPTIONAL DATA GRID (unused in your UI, but here for reference)
    # ----------------------------------------------------------------
    @render.data_frame
    def table():
        """
        Same data but displayed in a different way if needed—e.g., DataGrid.
        """
        df = dataset_data()
        if df.empty:
            return pd.DataFrame()

        df_mod = df.copy()
        df_mod["first_job_waiting_time"] = (df_mod["first_job_waiting_time"] / 60).round(2)
        df_mod.rename(columns={"first_job_waiting_time": "first_job_waiting_time (min)"}, inplace=True)
        return render.DataGrid(df_mod)

    # ----------------------------------------------------------------
    # 5) PLOTS
    # ----------------------------------------------------------------

    # ---- (A) Bar Plot: Median waiting time by job_type ----
    @render_plotly
    def barplot():
        df = dataset_data()
        if df.empty:
            print("No data available for bar plot in MPI Job")
            return go.Figure()

        color_var = input.scatter_color()

        df_plot = df.copy()
        # Remove prefix from job_type
        df_plot["job_type"] = df_plot["job_type"].str.replace("MPI job ", "", regex=False)
        # Convert sec -> min
        df_plot["first_job_waiting_time"] = (df_plot["first_job_waiting_time"] / 60).round(2)

        # Group by job_type, compute median
        grouped = df_plot.groupby("job_type")["first_job_waiting_time"].median().reset_index()

        fig = px.bar(
            grouped,
            x="job_type",
            y="first_job_waiting_time",
            color=None if color_var == "none" else color_var,
            labels={
                "first_job_waiting_time": "Median Waiting Time (min)",
                "job_type": "Job Type"
            },
            text_auto=".1f"
        )
        return fig

    # ---- (B) Box Plot: Job Waiting Time by Month & Year ----
    @render_plotly
    def job_waiting_time_by_month():
        df = dataset_data()
        if df.empty:
            return go.Figure()

        df_plot = df.copy()
        df_plot["job_type"] = df_plot["job_type"].str.replace("MPI job ", "", regex=False)
        df_plot["job_waiting_time (hours)"] = df_plot["first_job_waiting_time"] / 3600.0

        # Filter by selected years
        selected_years = list(map(int, input.years()))
        df_plot = df_plot[df_plot['year'].isin(selected_years)]

        # Limit the number of points per year
        max_points = 10000  # Set the maximum number of data points
        points_per_year = max_points // len(selected_years) if selected_years else max_points

        # Downsample each year
        downsampled_data = []
        for year in selected_years:
            year_data = df_plot[df_plot['year'] == year]
            if len(year_data) > points_per_year:
                year_data = year_data.sample(n=points_per_year, random_state=42)
            downsampled_data.append(year_data)

        # Combine downsampled data
        df_plot = pd.concat(downsampled_data) if downsampled_data else df_plot

        # Create the box plot
        fig = px.box(
            df_plot,
            x="month",
            y="job_waiting_time (hours)",
            color="year",
            facet_col="job_type",
            labels={"job_waiting_time (hours)": "Job Waiting Time (hours)"}
        )
        fig.update_layout(
            yaxis=dict(range=[0, 20]),
            boxmode="group",
            title=None,
            showlegend=True
        )

        # Remove "job_type=" prefix in facet titles
        for annotation in fig["layout"]["annotations"]:
            if annotation["text"].startswith("job_type="):
                annotation["text"] = annotation["text"][9:]
        # Remove x-axis label for subplots
        for axis in fig.layout:
            if axis.startswith("xaxis"):
                fig.layout[axis].title.text = None
        # Add jittered points
        fig.update_traces(
            marker=dict(size=6, opacity=0.7, line=dict(width=1, color="white")),
            boxpoints="all",
            jitter=0.3,
            pointpos=0
        )
        # Ensure correct month order
        fig.update_xaxes(categoryorder="array", categoryarray=month_order)
        return fig


    # ---- (C) Box Plot: Job Waiting Time by CPU Cores ----
    @render_plotly
    def job_waiting_time_by_cpu():
        df = dataset_data()
        if df.empty:
            return go.Figure()

        df_plot = df.copy()
        # Convert sec -> hours
        df_plot["job_waiting_time (hours)"] = df_plot["first_job_waiting_time"] / 3600.0
        # Convert slots to str for categorical plotting
        df_plot["slots"] = df_plot["slots"].astype(int).astype(str)

        # Filter by selected years
        selected_years = list(map(int, input.years()))
        df_plot = df_plot[df_plot['year'].isin(selected_years)]

        # Limit the number of points per year
        max_points = 10000  # Set the maximum number of data points
        points_per_year = max_points // len(selected_years) if selected_years else max_points

        # Downsample each year
        downsampled_data = []
        for year in selected_years:
            year_data = df_plot[df_plot['year'] == year]
            if len(year_data) > points_per_year:
                year_data = year_data.sample(n=points_per_year, random_state=42)
            downsampled_data.append(year_data)

        # Combine downsampled data
        df_plot = pd.concat(downsampled_data) if downsampled_data else df_plot

        # Create the box plot
        fig = px.box(
            df_plot,
            x="slots",
            y="job_waiting_time (hours)",
            color="year",
            labels={
                "slots": "CPU Cores",
                "job_waiting_time (hours)": "Job Waiting Time (hours)"
            }
        )
        fig.update_layout(
            yaxis=dict(range=[0, 20]),
            boxmode="group",
            title=None,
            showlegend=True
        )
        # Add jittered points
        fig.update_traces(
            marker=dict(size=6, opacity=0.7, line=dict(width=1, color="white")),
            boxpoints="all",
            jitter=0.3,
            pointpos=0
        )
        return fig


    # ----------------------------------------------------------------
    # 6) Reactive Effects for "Select All"/"Unselect All" (If needed)
    # ----------------------------------------------------------------
    @reactive.effect
    @reactive.event(input.select_all)
    def _():
        # Example: if you had a CPU selection UI, you might update it here
        pass

    @reactive.effect
    @reactive.event(input.unselect_all)
    def _():
        # Example: if you had a job_type or CPU selection UI, you might clear it here
        pass
