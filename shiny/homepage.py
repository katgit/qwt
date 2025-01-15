import faicons as fa
import pandas as pd
from shiny import ui, render, reactive
from shinywidgets import output_widget, render_plotly
import plotly.express as px
import plotly.graph_objects as go

# --------------------------------------------------------------------
# DATA LOADING & GLOBAL PREP
# --------------------------------------------------------------------

# Load data
dataset = pd.read_feather("/projectnb/rcs-intern/Jiazheng/accounting/ShinyApp_Data.feather")

# Ensure 'year' is integer
dataset["year"] = dataset["year"].astype(int)

# Define month ordering
month_order = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
]
dataset["month"] = pd.Categorical(dataset["month"], categories=month_order, ordered=True)

# Pre-define icons
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

# --------------------------------------------------------------------
# UI DEFINITION
# --------------------------------------------------------------------

def homepage_ui():
    """
    Build the UI for the homepage, including:
    - Sidebar with dynamic slider, job_type, and year checkbox groups
    - Value boxes for summary statistics
    - Main area with data table and multiple plots
    """
    return ui.page_sidebar(
        ui.sidebar(
            ui.output_ui("dynamic_slider"),  # Dynamically render slider
            ui.input_checkbox_group(
                "job_type",
                "Job Type",
                list(dataset["job_type"].unique()),
                selected=list(dataset["job_type"].unique()),
                inline=True,
            ),
            ui.input_action_button("select_all", "Select All"),
            ui.input_action_button("unselect_all", "Unselect All"),
            open="desktop",
        ),
        ui.input_checkbox_group(
            "years",  
            "Select Year(s)",
            list(range(2013, 2026)),  # Extend from 2013 to 2025
            selected=[2024],          # Default selection
            inline=True
        ),
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
        ui.layout_columns(
            # Data table card (if desired, uncomment later)
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
                    "3D Bubble Chart of Average Job Waiting Time by Year & Job Type",
                    class_="d-flex justify-content-between align-items-center"
                ),
                output_widget("job_waiting_time_3d"),
                full_screen=True
            ),
            col_widths=[6, 6, 6]
        ),
        fillable=True,
    )


# --------------------------------------------------------------------
# SERVER LOGIC
# --------------------------------------------------------------------

def homepage_server(input, output, session):
    """
    Server logic for homepage:
    - Reactive filtering based on years, job_type, and waiting_time
    - Calculation of dynamic slider range
    - Creation of summary statistics and plots
    """

    # ----------------------------------------------------------------
    # 1) Filter by year only, to determine slider range
    # ----------------------------------------------------------------
    @reactive.Calc
    def dataset_year_filtered():
        """
        Filter dataset by selected years for use in calculating the slider range.
        """
        years = list(map(int, input.years()))
        if years:
            df_years = dataset[dataset["year"].isin(years)]
        else:
            df_years = dataset.iloc[0:0]  # Empty
        return df_years

    # ----------------------------------------------------------------
    # 2) Compute slider range based on the year-filtered dataset
    # ----------------------------------------------------------------
    @reactive.Calc
    def formatted_range():
        """
        Decide the unit (Seconds, Minutes, or Hours) and compute
        the (min, max) range for waiting_time among the year-filtered data.
        """
        filtered_data = dataset_year_filtered()
        if filtered_data.empty:
            return (0, 0), "Seconds"

        min_time = max(filtered_data["first_job_waiting_time"].min(), 0)
        max_time = filtered_data["first_job_waiting_time"].max()

        # No valid data (e.g., all are NaN)
        if pd.isna(min_time) or pd.isna(max_time):
            return (0, 0), "Seconds"

        if max_time >= 3600:
            # Display slider in hours
            return (int(min_time // 3600), int(max_time // 3600) + 1), "Hours"
        elif max_time >= 60:
            # Display slider in minutes
            return (int(min_time // 60), int(max_time // 60) + 1), "Minutes"
        else:
            # Display slider in seconds
            return (int(min_time), int(max_time) + 1), "Seconds"

    # ----------------------------------------------------------------
    # 3) Dynamic slider UI
    # ----------------------------------------------------------------
    @output
    @render.ui
    def dynamic_slider():
        """
        Dynamically render the slider UI based on the (range, unit).
        """
        (range_min, range_max), unit_label = formatted_range()
        # Ensure non-negative min
        range_min = max(range_min, 0)

        return ui.input_slider(
            "first_job_waiting_time",
            f"Waiting Time ({unit_label})",
            min=range_min,
            max=range_max,
            value=(range_min, range_max),
        )

    # ----------------------------------------------------------------
    # 4) Final reactive filter: (years + job_type + waiting_time)
    # ----------------------------------------------------------------
    @reactive.Calc
    def dataset_data():
        """
        The fully filtered dataset used for tables & plots.
        Filters:
          1) year in input.years()
          2) job_type in input.job_type()
          3) waiting_time within the selected slider range
        """
        (range_min, range_max), unit_label = formatted_range()
        slider_min, slider_max = input.first_job_waiting_time()
        years = list(map(int, input.years()))
        job_types = input.job_type()

        # Convert slider values back to seconds
        if unit_label == "Hours":
            min_sec = slider_min * 3600
            max_sec = slider_max * 3600
        elif unit_label == "Minutes":
            min_sec = slider_min * 60
            max_sec = slider_max * 60
        else:  # "Seconds"
            min_sec = slider_min
            max_sec = slider_max

        # Filter
        df = dataset[dataset["year"].isin(years)]
        df = df[df["first_job_waiting_time"].between(min_sec, max_sec)]
        df = df[df["job_type"].isin(job_types)]

        return df[["job_type", "first_job_waiting_time", "month", 
                    "job_number", "year", "slots"]]

    # ----------------------------------------------------------------
    # 5) Summary stats (min, max, mean, median, count)
    # ----------------------------------------------------------------
    @reactive.Calc
    def waiting_time_stats():
        """
        Return a dict of summary statistics for the filtered data.
        Times are stored in minutes for easy conversion.
        """
        df = dataset_data()
        if df.empty:
            return {"min": None, "max": None, "mean": None, "median": None, "count": 0}

        waiting_times_sec = df["first_job_waiting_time"]
        stats = {
            "min": max(waiting_times_sec.min() / 60.0, 0),
            "max": waiting_times_sec.max() / 60.0,
            "mean": waiting_times_sec.mean() / 60.0,
            "median": waiting_times_sec.median() / 60.0,
            "count": df.shape[0],
        }
        return stats

    # ----------------------------------------------------------------
    # Value box outputs
    # ----------------------------------------------------------------
    @output
    @render.text
    def min_waiting_time():
        stats = waiting_time_stats()
        if stats["min"] is None:
            return "No data available"
        min_val = stats["min"]
        return f"{min_val / 60:.1f} hours" if min_val > 60 else f"{min_val:.1f} min"

    @output
    @render.text
    def max_waiting_time():
        stats = waiting_time_stats()
        if stats["max"] is None:
            return "No data available"
        max_val = stats["max"]
        return f"{max_val / 60:.1f} hours" if max_val > 60 else f"{max_val:.1f} min"

    @output
    @render.text
    def mean_waiting_time():
        stats = waiting_time_stats()
        if stats["mean"] is None:
            return "No data available"
        mean_val = stats["mean"]
        return f"{mean_val / 60:.1f} hours" if mean_val > 60 else f"{mean_val:.1f} min"

    @output
    @render.text
    def median_waiting_time():
        stats = waiting_time_stats()
        if stats["median"] is None:
            return "No data available"
        median_val = stats["median"]
        return f"{median_val / 60:.1f} hours" if median_val > 60 else f"{median_val:.1f} min"

    @output
    @render.text
    def job_count():
        stats = waiting_time_stats()
        return str(stats["count"])

    # ----------------------------------------------------------------
    # Main data table (if you want to show it, uncomment)
    # ----------------------------------------------------------------
    # @output
    # @render.data_frame
    # def displayTable():
    #     """
    #     Display the filtered data in a table, with waiting time shown in minutes.
    #     """
    #     data = dataset_data().copy()
    #     # Convert to minutes for display
    #     data["first_job_waiting_time"] = (data["first_job_waiting_time"] / 60).round(1)
    #     # Sort by job_number
    #     data.sort_values(by="job_number", ascending=True, inplace=True)
    #     # Rename columns for user-friendly display
    #     data_renamed = data.rename(
    #         columns={
    #             "job_type": "Job Type",
    #             "first_job_waiting_time": "Waiting Time (min)",
    #             "month": "Month",
    #             "job_number": "Job Number",
    #             "year": "Year",
    #             "slots": "CPU Cores",
    #         }
    #     )
    #     return data_renamed

    # ----------------------------------------------------------------
    # PLOTS
    # ----------------------------------------------------------------

    # 1) Bar plot
    @render_plotly
    def barplot():
        """
        Median waiting time (minutes) by job type, with optional coloring.
        """
        data = dataset_data()
        if data.empty:
            return go.Figure()

        # Handle color option
        color_option = input.scatter_color()

        # Convert waiting time to minutes
        df = data.copy()
        df["first_job_waiting_time"] = (df["first_job_waiting_time"] / 60).round(2)

        # Simplify job types for nicer grouping
        df.loc[df["job_type"].str.contains("1-p", na=False), "job_type"] = "1-P"
        df.loc[df["job_type"].isin(["GPU > 1", "GPU = 1"]), "job_type"] = "GPU"
        df.loc[df["job_type"].str.contains("MPI", na=False), "job_type"] = "MPI"
        df.loc[df["job_type"].str.contains("OMP", na=False), "job_type"] = "OMP"

        # Calculate medians
        medians = (
            df.groupby("job_type")["first_job_waiting_time"]
            .median()
            .reset_index()
        )

        # Create bar plot
        fig = px.bar(
            medians,
            x="job_type",
            y="first_job_waiting_time",
            color=None if color_option == "none" else color_option,
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
        Box plot of waiting time (in hours) by month, colored by year.
        Limit the number of points displayed to at most 2000 for performance and clarity.
        """
        data = dataset_data()
        if data.empty:
            return go.Figure()

        df = data.copy()
        df["job_waiting_time_hours"] = df["first_job_waiting_time"] / 3600.0

        # Define the maximum number of points to display
        max_points = 2000

        if len(df) > max_points:
            # Group data by month and year and aggregate similar points
            grouped = (
                df.groupby(["month", "year"])
                .apply(lambda g: g.sample(n=min(len(g), max_points // len(df["month"].unique())), random_state=42))
                .reset_index(drop=True)
            )

            # Add outliers back to the dataset
            outliers = df[
                (df["job_waiting_time_hours"] < df["job_waiting_time_hours"].quantile(0.05)) |
                (df["job_waiting_time_hours"] > df["job_waiting_time_hours"].quantile(0.95))
            ]
            df = pd.concat([grouped, outliers]).drop_duplicates().reset_index(drop=True)

        fig = px.box(
            df,
            x="month",
            y="job_waiting_time_hours",
            color="year",
            labels={"job_waiting_time_hours": "Job Waiting Time (hours)"},
        )

        # Ensure the correct month order in x-axis
        fig.update_xaxes(categoryorder="array", categoryarray=month_order)

        # Layout adjustments
        fig.update_layout(
            boxmode="group",
            showlegend=True
        )

        return fig


    # 3) 3D bubble chart
    @render_plotly
    def job_waiting_time_3d():
        """
        3D scatter of average job waiting time (hours) by year, month, and job type.
        Bubble size encodes waiting time as well.
        """
        data = dataset_data()
        if data.empty:
            return go.Figure()

        # Compute average waiting time by (year, month, job_type)
        df = data.groupby(["year", "month", "job_type"])["first_job_waiting_time"].mean().reset_index()
        df["waiting_time_hours"] = df["first_job_waiting_time"] / 3600.0
        df["waiting_time_hours"].fillna(0, inplace=True)

        # Ensure correct month order
        df["month"] = pd.Categorical(df["month"], categories=month_order, ordered=True)

        fig = px.scatter_3d(
            df,
            x="month",
            y="job_type",
            z="waiting_time_hours",
            size="waiting_time_hours",
            color="month",
            hover_data=["year", "job_type"],
            labels={"waiting_time_hours": "Waiting Time (hours)"},
        )
        fig.update_layout(
            scene=dict(
                xaxis=dict(
                    tickmode="array",
                    tickvals=list(range(len(month_order))),
                    ticktext=month_order,
                ),
                zaxis=dict(title="Waiting Time (hours)"),
            ),
            scene_zaxis_type="linear",
        )
        return fig

    # ----------------------------------------------------------------
    # "Select All" & "Unselect All" event handlers
    # ----------------------------------------------------------------
    @reactive.effect
    @reactive.event(input.select_all)
    def _():
        """
        Select all job types when the user clicks 'Select All'.
        """
        all_job_types = list(dataset["job_type"].unique())
        ui.update_checkbox_group("job_type", selected=all_job_types)

    @reactive.effect
    @reactive.event(input.unselect_all)
    def _():
        """
        Unselect all job types when the user clicks 'Unselect All'.
        """
        ui.update_checkbox_group("job_type", selected=[])
