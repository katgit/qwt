import faicons as fa
import pandas as pd
from shiny import ui, render, reactive
from shinywidgets import output_widget, render_plotly
import plotly.express as px
import plotly.graph_objects as go
import datetime


# Load data
dataset = pd.read_feather("/projectnb/rcs-intern/Jiazheng/accounting/ShinyApp_Data.feather")
# Simplify job types for nicer grouping
dataset = dataset.copy()

# Define a mapping for job type simplification based on prefixes
job_type_mapping = {
    "1-p": "1-P",  # Map job types starting with "1-p" to "1-P"
    "GPU": "GPU",  # Map job types starting with "GPU" to "GPU"
    "MPI": "MPI",  # Map job types starting with "MPI" to "MPI"
    "OMP": "OMP"   # Map job types starting with "OMP" to "OMP"
}

# Apply the mapping using a lambda function
dataset["job_type"] = dataset["job_type"].apply(
    lambda x: next((v for k, v in job_type_mapping.items() if str(x).startswith(k)), x))

# Handle empty strings (e.g., "   " becomes NaN):
dataset["job_type"] = dataset["job_type"].replace("", pd.NA)

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
# UI 
# --------------------------------------------------------------------
now = datetime.datetime.now()

# custom value box to display icon (title output_id) horizontally
def value_box_custom(title, output_id, icon):
    return ui.value_box(
        "",
        ui.div(
            ui.div(
                ui.div(icon, class_="value-box-showcase custom-icon"),
                ui.div(
                    ui.div(title, class_="value-box-title"),
                    ui.div(ui.output_text(output_id), class_="value-box-value"),
                    class_="custom-text"
                ),
                class_="d-flex align-items-center gap-2"
            )
        )
    )

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
        ui.output_ui("warning_message"),
        ui.div(  # <== this replaces the layout_columns for inputs
            ui.div(
                ui.input_text(
                    "selected_year",
                    "Enter Year",
                    value=str(now.year),
                    placeholder="e.g., 2024"
                ),
                style="margin-right: 20px; width: 250px;"
            ),
            ui.div(
                ui.input_text(
                    "selected_month",
                    "Enter Month (e.g., Jan, Feb)",
                    value=now.strftime("%b"),
                    placeholder="e.g., Jan"
                ),
                style="margin-right: 20px; width: 250px;"
            ),
            ui.div(
                ui.input_select(
                    "queue_filter",
                    "Queue Type",
                    choices={
                        "all": "All",
                        "shared": "Shared Nodes Only",
                        "buyin": "Buyin Nodes Only"
                    },
                    selected="all"
                ),
                style="width: 250px;"
            ),
            style="display: flex; align-items: flex-end; margin-bottom: 1em;"
        ),
        ui.tags.style("""
            .custom-icon {
                align-items: center;
                justify-content: center;
            }

            .custom-text {
                flex-grow: 1;
                display: flex;
                flex-direction: column;
                justify-content: center;
            }

            .bslib-value-box .value-box-title {
                margin-top: 0;
                margin-bottom: 0rem;
            }
            .bslib-value-box .value-box-value {
                margin-bottom: 0rem;
            }

            .bslib-value-box .value-box-showcase,
            .bslib-value-box .value-box-showcase > .html-fill-item {
                width: unset !important;
                padding: 0rem;
            }
            
            .bslib-value-box .value-box-area {
                padding: 0 !important;
            }
        """),
        ui.layout_columns(
            value_box_custom("Min Waiting Time", "min_waiting_time", ICONS["min"]),
            value_box_custom("Max Waiting Time", "max_waiting_time", ICONS["max"]),
            value_box_custom("Mean Waiting Time", "mean_waiting_time", ICONS["speed"]),
            value_box_custom("Median Waiting Time", "median_waiting_time", ICONS["median"]),
            value_box_custom("Number of Jobs", "job_count", ICONS["count"]),
            fill=False,
        ),
        ui.layout_columns(
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
                output_widget("all_jobs_barplot"),
                full_screen=True
            ),
            ui.card(
                ui.card_header(
                    "Box Plot of Job Waiting Time by Date",
                    class_="d-flex justify-content-between align-items-center"
                ),
                output_widget("job_waiting_time_by_date"),
                full_screen=True
            ),
            col_widths=[6, 6]
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
        # try:
        #     year = int(input.selected_year())
        # except ValueError:
        #     year = now.year  # fallback to current year
        # df_years = dataset[dataset["year"] == year]
        # return df_years  # <- This was missing
        year, month, warning = selected_year_month()
        if warning or year is None:
            return dataset.iloc[0:0]  # return empty DataFrame
        return dataset[dataset["year"] == year]


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
        # Prevent slider rendering if range is invalid
        if range_max <= range_min:
            return ui.markdown("⚠️ No data available to display slider.")

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

        year, month, warning = selected_year_month()
        if warning or year is None:
            return dataset.iloc[0:0]

        # Convert slider values back to seconds
        if unit_label == "Hours":
            min_sec = slider_min * 3600
            max_sec = slider_max * 3600
        elif unit_label == "Minutes":
            min_sec = slider_min * 60
            max_sec = slider_max * 60
        else:
            min_sec = slider_min
            max_sec = slider_max

        job_types = input.job_type()

        df = dataset[(dataset["year"] == year) & (dataset["month"] == month)]
        df = df[df["first_job_waiting_time"].between(min_sec, max_sec)]
        df = df[df["job_type"].isin(job_types)]

        queue_filter = input.queue_filter()
        if queue_filter == "shared":
            df = df[df["queue_type"] == "shared"]
        elif queue_filter == "buyin":
            df = df[df["queue_type"] == "buyin"]

        return df[["job_type", "first_job_waiting_time", "month", "job_number", "year", "slots"]]

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
    # PLOTS
    # ----------------------------------------------------------------

    # 1) Bar plot
    @render_plotly
    def all_jobs_barplot():
        """
        Median waiting time (minutes) by job type, with optional coloring.
        """
        data = dataset_data()
        if data.empty:
            return go.Figure()

        # Handle color option
        color_option = input.scatter_color()

        # Convert waiting time to minutes
        df = data
        df["first_job_waiting_time"] = (df["first_job_waiting_time"] / 60).round(2)

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
            text=medians["first_job_waiting_time"].apply(lambda x: f"{x:.1f}" if x != 0 else "0")  # Explicitly set text if the value is 0x
        )

        # Adjust text position and appearance
        fig.update_traces(textposition='outside', textfont_size=12)

        return fig

    @render_plotly
    def job_waiting_time_by_date():
        """
        Box plot of waiting time (in hours) by submission date (1–31), colored by year.
        Limits to 2000 points for performance.
        """
        data = dataset_data()
        if data.empty:
            return go.Figure()

        df = data.copy()

        # Ensure we have a 'submit_date' column as datetime
        if "submit_date" not in df.columns:
            return go.Figure()  # Or raise an error

        df["submit_date"] = pd.to_datetime(df["submit_date"])
        df["day"] = df["submit_date"].dt.day  # Extract 1–31
        df["job_waiting_time_hours"] = df["first_job_waiting_time"] / 3600.0

        # Downsample if needed
        max_points = 2000
        if len(df) > max_points:
            df = df.sample(n=max_points, random_state=42)

        # Plot by day of the month
        fig = px.box(
            df,
            x="day",
            y="job_waiting_time_hours",
            color="year",
            labels={
                "day": "Day of Month",
                "job_waiting_time_hours": "Job Waiting Time (hours)"
            },
            category_orders={"day": list(range(1, 32))}
        )

        fig.update_layout(
            boxmode="group",
            showlegend=True,
            xaxis=dict(tickmode="linear", dtick=1)
        )

        return fig


    @reactive.Calc
    def selected_year_month():
        try:
            year = int(input.selected_year())
        except ValueError:
            year = now.year  # fallback if invalid

        month = input.selected_month().capitalize()

        # Check if the month is valid
        if month not in month_order:
            return None, None, "Invalid month format. Please use 3-letter month (e.g., Jan, Feb)."

        # Check for future data
        latest_year = dataset["year"].max()
        future_check = (
            year > latest_year or
            (year == latest_year and month_order.index(month) > dataset[dataset["year"] == year]["month"].cat.codes.max())
        )

        if future_check:
            return year, month, "No data available for this month."

        return year, month, None


    # ----------------------------------------------------------------
    # "Select All" & "Unselect All" handlers
    # ----------------------------------------------------------------
    @reactive.effect
    @reactive.event(input.select_all)
    def _():
        all_job_types = list(dataset["job_type"].unique())
        ui.update_checkbox_group("job_type", selected=all_job_types)

    @reactive.effect
    @reactive.event(input.unselect_all)
    def _():
        ui.update_checkbox_group("job_type", selected=[])

    @output
    @render.ui
    def warning_message():
        _, _, warning = selected_year_month()
        if warning:
            return ui.markdown(f"**⚠️ Warning:** {warning}")
        return None