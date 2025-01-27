import faicons as fa
import pandas as pd
from shiny import ui, render, reactive
from shinywidgets import output_widget, render_plotly
import plotly.express as px
import plotly.graph_objects as go

# ---------------------------------------
# DATA LOADING
# ---------------------------------------
dataset = pd.read_feather("/projectnb/rcs-intern/Jiazheng/accounting/ShinyApp_Data_OneP.feather")

# Ensure 'year' is integer
# Optional: Remove rows with NaN values if needed
dataset.dropna(inplace=True)
dataset['year'] = dataset['year'].astype(int)

# Set month order
month_order = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
]
dataset['month'] = pd.Categorical(dataset['month'], categories=month_order, ordered=True)

# ---------------------------------------
# ICONS
# ---------------------------------------
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

# ---------------------------------------
# UI DEFINITION
# ---------------------------------------
def oneP_job_ui():
    """
    UI for the 1-p Job page.
    """
    return ui.page_fluid(
        ui.input_checkbox_group(
            "years",
            "Select Year(s)",
            list(range(2013, 2026)),
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
            ui.card(
                ui.card_header(
                    "Waiting Time vs Job Type",
                    class_="d-flex justify-content-between align-items-center"
                ),
                output_widget("waiting_time_vs_job_type"),
                full_screen=True
            ),
            ui.card(
                ui.card_header(
                    "Box Plot of Job Waiting Time by Month & Year",
                    class_="d-flex justify-content-between align-items-center"
                ),
                output_widget("job_waiting_time_by_month_year"),
                full_screen=True
            ),
            col_widths=[6, 6]
        ),
        fillable=True,
    )

# ---------------------------------------
# SERVER LOGIC
# ---------------------------------------
def oneP_job_server(input, output, session):
    """
    Server logic for the 1-p Job page.
    """

    @reactive.Calc
    def oneP_filtered_data():
        """
        Filter the dataset to the selected years and job type '1-p'.
        """
        years = list(map(int, input.years()))
        if not years:
            return dataset.iloc[0:0]
        return dataset[dataset["year"].isin(years)]

    @reactive.Calc
    def waiting_time_stats():
        """
        Calculate summary stats for the filtered dataset.
        """
        df = oneP_filtered_data()
        if df.empty:
            return dict(min=None, max=None, mean=None, median=None, count=0)

        waiting_times = df["first_job_waiting_time"] / 60.0
        return dict(
            min=max(waiting_times.min(), 0),
            max=waiting_times.max(),
            mean=waiting_times.mean(),
            median=waiting_times.median(),
            count=len(waiting_times)
        )

    @output
    @render.text
    def min_waiting_time():
        stats = waiting_time_stats()
        if stats["min"] is None:
            return "No data available"
        return f"{stats['min'] / 60:.1f} hours" if stats["min"] > 60 else f"{stats['min']:.1f} min"

    @output
    @render.text
    def max_waiting_time():
        stats = waiting_time_stats()
        if stats["max"] is None:
            return "No data available"
        return f"{stats['max'] / 60:.1f} hours" if stats["max"] > 60 else f"{stats['max']:.1f} min"

    @output
    @render.text
    def mean_waiting_time():
        stats = waiting_time_stats()
        if stats["mean"] is None:
            return "No data available"
        return f"{stats['mean'] / 60:.1f} hours" if stats["mean"] > 60 else f"{stats['mean']:.1f} min"

    @output
    @render.text
    def median_waiting_time():
        stats = waiting_time_stats()
        if stats["median"] is None:
            return "No data available"
        return f"{stats['median'] / 60:.1f} hours" if stats["median"] > 60 else f"{stats['median']:.1f} min"

    @output
    @render.text
    def job_count():
        stats = waiting_time_stats()
        return str(stats["count"])

    @render_plotly
    def waiting_time_vs_job_type():
        """
        Bar plot of median waiting time by job type.
        """
        df = oneP_filtered_data()
        if df.empty:
            return go.Figure()

        df_plot = df.copy()
        df_plot["job_type"] = df_plot["job_type"].str.replace("1-p ", "", regex=False)
        df_plot["waiting_time_hours"] = df_plot["first_job_waiting_time"] / 3600

        grouped = df_plot.groupby("job_type")["waiting_time_hours"].median().reset_index()

        # Group job types with median waiting time < 2 together
        grouped["job_type"] = grouped.apply(
            lambda row: "Others (<1 hrs)" if row["waiting_time_hours"] < 1 else row["job_type"], axis=1
        )

        grouped = grouped.groupby("job_type")["waiting_time_hours"].median().reset_index()
        grouped = grouped.sort_values(by="waiting_time_hours", ascending=True)

        fig = px.bar(
            grouped,
            x="job_type",
            y="waiting_time_hours",
            labels={"waiting_time_hours": "Median Waiting Time (hours)", "job_type": "Job Type"},
            title="Median Waiting Time by Job Type"
        )
        return fig

    @render_plotly
    def job_waiting_time_by_month_year():
        """
        Box plot of job waiting time by month and year.
        """
        df = oneP_filtered_data()
        if df.empty:
            return go.Figure()

        df_plot = df.copy()
        df_plot["waiting_time_hours"] = df_plot["first_job_waiting_time"] / 3600

        fig = px.box(
            df_plot,
            x="month",
            y="waiting_time_hours",
            color="year",
            category_orders={"month": month_order},
            title="Job Waiting Time by Month & Year",
            labels={"waiting_time_hours": "Waiting Time (hours)", "month": "Month"}
        )
        return fig
