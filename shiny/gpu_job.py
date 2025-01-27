import faicons as fa
import pandas as pd
from shiny import ui, render, reactive
from shinywidgets import output_widget, render_plotly
import plotly.express as px
import plotly.graph_objects as go  # For empty Figure

# --------------------------------------------------------------------
# DATA LOADING & PREP
# --------------------------------------------------------------------

dataset = pd.read_feather("/projectnb/rcs-intern/Jiazheng/accounting/ShinyApp_Data_GPU.feather")

# Ensure 'year' column is integer
dataset["year"] = dataset["year"].astype(int)

# Define ordered months
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
    "mean": fa.icon_svg("users"),   # Alternatively, "speed" for consistency
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

# --------------------------------------------------------------------
# UI DEFINITION
# --------------------------------------------------------------------

def gpu_job_ui():
    """
    UI for the GPU Job page: 
    - Year selection
    - Summary stats
    - Data table
    - Various plots
    """
    return ui.page_fluid(
        # ------------------ Year Selection ------------------
        ui.input_checkbox_group(
            "years",
            "Select Year(s)",
            list(range(2013, 2026)),  # 2013–2025
            selected=[2024],
            inline=True
        ),

        # ------------------ Value Boxes ---------------------
        ui.layout_columns(
            ui.value_box(
                "Min Waiting Time", ui.output_text("min_waiting_time"), showcase=ICONS["min"]
            ),
            ui.value_box(
                "Max Waiting Time", ui.output_text("max_waiting_time"), showcase=ICONS["max"]
            ),
            ui.value_box(
                "Mean Waiting Time", ui.output_text("mean_waiting_time"), showcase=ICONS["speed"]
            ),
            ui.value_box(
                "Median Waiting Time", ui.output_text("median_waiting_time"), showcase=ICONS["median"]
            ),
            ui.value_box(
                "Number of Jobs", ui.output_text("job_count"), showcase=ICONS["count"]
            ),
            fill=False,
        ),

        # ------------------ Main Content ---------------------
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
            col_widths=[6, 6, 6]
        ),
        fillable=True,
    )

# --------------------------------------------------------------------
# SERVER LOGIC
# --------------------------------------------------------------------

def gpu_job_server(input, output, session):
    """
    Server logic for GPU Job page:
      1) Filter dataset by selected years (and optionally job_type if needed).
      2) Compute summary stats (min, max, mean, median, count).
      3) Render data table and various plots (barplot, boxplot).
    """

    # ------------------ 1) Reactive Filter ------------------
    @reactive.Calc
    def gpu_data():
        """
        Filter the global 'dataset' based on selected years.
        (If you need to filter by GPU job_type, you can do it here, e.g.:
         df = df[df["job_type"].str.contains("GPU")])
        """
        selected_years = list(map(int, input.years()))
        if not selected_years:
            return dataset.iloc[0:0]  # Return empty if no years selected

        df_filtered = dataset[dataset["year"].isin(selected_years)]
        return df_filtered

    # ------------------ 2) Summary Stats ------------------
    @reactive.Calc
    def gpu_waiting_time_stats():
        """
        Calculate min, max, mean, median, and count for first_job_waiting_time
        (in minutes). Returns a dictionary of stats.
        """
        df = gpu_data()
        if df.empty:
            return {"min": None, "max": None, "mean": None, "median": None, "count": 0}

        # Convert from seconds -> minutes
        times_min = df["first_job_waiting_time"] / 60.0
        return {
            "min": max(times_min.min(), 0),
            "max": times_min.max(),
            "mean": times_min.mean(),
            "median": times_min.median(),
            "count": df.shape[0],
        }

    # ------------------ 3) Value Box Renderers ------------------
    @output
    @render.text
    def min_waiting_time():
        stats = gpu_waiting_time_stats()
        if stats["min"] is None:
            return "No data available"
        return f"{stats['min'] / 60:.1f} hours" if stats["min"] > 60 else f"{stats['min']:.1f} min"

    @output
    @render.text
    def max_waiting_time():
        stats = gpu_waiting_time_stats()
        if stats["max"] is None:
            return "No data available"
        return f"{stats['max'] / 60:.1f} hours" if stats["max"] > 60 else f"{stats['max']:.1f} min"

    @output
    @render.text
    def mean_waiting_time():
        stats = gpu_waiting_time_stats()
        if stats["mean"] is None:
            return "No data available"
        return f"{stats['mean'] / 60:.1f} hours" if stats["mean"] > 60 else f"{stats['mean']:.1f} min"

    @output
    @render.text
    def median_waiting_time():
        stats = gpu_waiting_time_stats()
        if stats["median"] is None:
            return "No data available"
        return f"{stats['median'] / 60:.1f} hours" if stats["median"] > 60 else f"{stats['median']:.1f} min"

    @output
    @render.text
    def job_count():
        stats = gpu_waiting_time_stats()
        return str(stats["count"])

    # ------------------ 4) Data Table ------------------
    # @output
    # @render.data_frame
    # def displayTable():
    #     """
    #     Show the filtered data in a table, converting waiting time to minutes (string).
    #     """
    #     df = gpu_data()
    #     if df.empty:
    #         return pd.DataFrame()

    #     df_disp = df.copy()
    #     # Convert to minutes (string format)
    #     df_disp["first_job_waiting_time"] = (df_disp["first_job_waiting_time"] / 60).round(1).astype(str)
    #     # Sort by job_number
    #     df_disp.sort_values("job_number", ascending=True, inplace=True)

    #     # Rename columns for display
    #     df_disp.rename(
    #         columns={
    #             "job_type": "Job Type",
    #             "first_job_waiting_time": "Waiting Time (min)",
    #             "month": "Month",
    #             "job_number": "Job Number",
    #             "year": "Year",
    #             "slots": "CPU Cores",
    #         },
    #         inplace=True
    #     )
    #     return df_disp

    # ------------------ 5) Plots ------------------

    @render_plotly
    def barplot():
        """
        Bar plot of median waiting time (minutes) by job_type.
        Optional color variable from radio buttons: job_type or none.
        """
        df = gpu_data()
        if df.empty:
            return go.Figure()

        # Convert sec -> min
        df = df[df["first_job_waiting_time"] >= 0].copy()
        df["first_job_waiting_time"] = (df["first_job_waiting_time"] / 60).round(2)

        # Group by job_type for median
        grouped = df.groupby("job_type")["first_job_waiting_time"].median().reset_index()

        color_var = input.scatter_color()
        fig = px.bar(
            grouped,
            x="job_type",
            y="first_job_waiting_time",
            color=None if color_var == "none" else color_var,
            labels={
                "first_job_waiting_time": "Median Waiting Time (min)",
                "job_type": "Job Type",
            },
            text_auto=".1f"
        )
        return fig

    @render_plotly
    def job_waiting_time_by_month():
        """
        Box plot of waiting time (hours) by month, colored by year,
        and faceted by job_type if multiple job types exist.
        """
        df = gpu_data()
        if df.empty:
            return go.Figure()

        df_plot = df.copy()

        # Handle missing data upfront
        df_plot.dropna(subset=["first_job_waiting_time", "month", "year", "job_type"], inplace=True)

        # Convert waiting time to hours
        df_plot["job_waiting_time (hours)"] = df_plot["first_job_waiting_time"] / 3600.0

        # Downsample for large datasets
        max_points = 10000
        if len(df_plot) > max_points:
            df_plot = df_plot.sample(n=max_points, random_state=42)

        # Determine if faceting is necessary
        unique_job_types = df_plot["job_type"].nunique()
        facet_col = "job_type" if unique_job_types > 1 else None

        # Create the box plot
        fig = px.box(
            df_plot,
            x="month",
            y="job_waiting_time (hours)",
            color="year",
            facet_col=facet_col,
            labels={
                "job_waiting_time (hours)": "Job Waiting Time (hours)",
                "month": "Month",
            },
            category_orders={"month": month_order}  # Ensure correct month order
        )

        # Layout adjustments
        fig.update_layout(
            boxmode="group",
            yaxis=dict(range=[0, 20]),
            showlegend=True
        )

        # Remove 'job_type=' prefix if faceted
        if facet_col:
            fig.for_each_annotation(lambda a: a.update(text=a.text.replace("job_type=", "")))

        # Add jittered points for visibility
        fig.update_traces(
            marker=dict(size=6, opacity=0.7, line=dict(width=1, color="white")),
            boxpoints="all",
            jitter=0.3,
            pointpos=0
        )

        # Return the figure
        return fig


    # ------------------ 6) "Select All" / "Unselect All" ------------------
    @reactive.effect
    @reactive.event(input.select_all)
    def _():
        """
        Example: If you had a job_type checkbox group for GPU,
        you could select all GPU job types here.
        """
        pass

    @reactive.effect
    @reactive.event(input.unselect_all)
    def _():
        """
        Example: Clear selections, e.g. job_type or years.
        """
        pass
