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
    "GPU": "GPU", 
    "MPI": "MPI",  
    "OMP": "OMP"  
}

# Apply the mapping using a lambda function
dataset["job_type"] = dataset["job_type"].apply(
    lambda x: next((v for k, v in job_type_mapping.items() if str(x).startswith(k)), x))

# Handle empty strings
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

# UI 
now = datetime.datetime.now()
PAGE_ID = "homepage"
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

def homepage_ui(selected_year, selected_month):
    # Build the UI for the homepage, including:
    return ui.page_sidebar(
        ui.sidebar(
            ui.output_ui(f"{PAGE_ID}_dynamic_slider"),  # Dynamically render slider
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
        ui.output_ui(f"{PAGE_ID}_warning_message"),
        ui.div( 
            ui.div(
                ui.input_text(
                    "selected_year",
                    "Enter Year",
                    value=selected_year.get(),
                    placeholder="e.g., 2024"
                ),
                style="margin-right: 20px; width: 250px;"
            ),
            ui.div(
                ui.input_text(
                    "selected_month",
                    "Enter Month (e.g., Jan, Feb)",
                    value=selected_month.get(),
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
            value_box_custom("Min Waiting Time", f"{PAGE_ID}_min_waiting_time", ICONS["min"]),
            value_box_custom("Max Waiting Time", f"{PAGE_ID}_max_waiting_time", ICONS["max"]),
            value_box_custom("Mean Waiting Time", f"{PAGE_ID}_mean_waiting_time", ICONS["speed"]),
            value_box_custom("Median Waiting Time", f"{PAGE_ID}_median_waiting_time", ICONS["median"]),
            value_box_custom("Number of Jobs", f"{PAGE_ID}_job_count", ICONS["count"]),
            fill=False,
        ),
        ui.layout_columns(
            ui.card(
                ui.card_header(
                    "Waiting Time vs Job Type",
                    ui.popover(
                        ICONS["ellipsis"],
                        ui.input_radio_buttons(
                            "homepage_scatter_color",
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
        ),
        fillable=True,
    )


# SERVER LOGIC
def homepage_server(input, output, session, selected_year, selected_month):

    
    # 1) Filter by year only, to determine slider range
    @reactive.Calc
    def dataset_year_filtered():
        year, month, warning = selected_year_month()
        if warning or year is None:
            return dataset.iloc[0:0]  # return empty DataFrame
        return dataset[(dataset["year"] == year) & (dataset["month"] == month)]


    # 2) Compute slider range based on the year-filtered dataset
    @reactive.Calc
    def formatted_range():
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
            return (int(min_time // 60), int(max_time // 60) + 1), "Minutes"
        else:
            return (int(min_time), int(max_time) + 1), "Seconds"

    # 3) Dynamic slider UI
    @output(id=f"{PAGE_ID}_dynamic_slider")
    @render.ui
    def dynamic_slider():
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

    # 4) Final reactive filter: (years + job_type + waiting_time)
    @reactive.Calc
    def dataset_data():
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
            df = df[df["class_user"] == "shared"]
        elif queue_filter == "buyin":
            df = df[(df["class_own"] == "buyin") & (df["class_user"] == "buyin")]

        return df[["job_type", "first_job_waiting_time", "day", "month", "job_number", "year", "slots"]]

    # 5) Summary stats (min, max, mean, median, count)
    @reactive.Calc
    def waiting_time_stats():
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

    # Value box outputs
    @output(id=f"{PAGE_ID}_min_waiting_time")
    @render.text
    def min_waiting_time():
        stats = waiting_time_stats()
        if stats["min"] is None:
            return "No data available"
        min_val = stats["min"]
        return f"{min_val / 60:.1f} hours" if min_val > 60 else f"{min_val:.1f} min"

    @output(id=f"{PAGE_ID}_max_waiting_time")
    @render.text
    def max_waiting_time():
        stats = waiting_time_stats()
        if stats["max"] is None:
            return "No data available"
        max_val = stats["max"]
        return f"{max_val / 60:.1f} hours" if max_val > 60 else f"{max_val:.1f} min"

    @output(id=f"{PAGE_ID}_mean_waiting_time")
    @render.text
    def mean_waiting_time():
        stats = waiting_time_stats()
        if stats["mean"] is None:
            return "No data available"
        mean_val = stats["mean"]
        return f"{mean_val / 60:.1f} hours" if mean_val > 60 else f"{mean_val:.1f} min"

    @output(id=f"{PAGE_ID}_median_waiting_time")
    @render.text
    def median_waiting_time():
        stats = waiting_time_stats()
        if stats["median"] is None:
            return "No data available"
        median_val = stats["median"]
        return f"{median_val / 60:.1f} hours" if median_val > 60 else f"{median_val:.1f} min"

    @output(id=f"{PAGE_ID}_job_count")
    @render.text
    def job_count():
        stats = waiting_time_stats()
        return str(stats["count"])

    
    # PLOTS

    # 1) Bar plot
    @output(id="all_jobs_barplot")
    @render_plotly
    def all_jobs_barplot():
        if "selected_navset_bar" in input and input.selected_navset_bar() != "All Jobs":
            return None

        data = dataset_data()
        if data.empty:
            return go.Figure()

        color_option = input.homepage_scatter_color()

        # Convert to minutes
        df = data.copy()
        df["first_job_waiting_time"] = (df["first_job_waiting_time"] / 60).round(2)

        # Compute medians
        medians = (
            df.groupby("job_type")["first_job_waiting_time"]
            .median()
            .reset_index()
        )

        # Create plot
        fig = px.bar(
            medians,
            x="job_type",
            y="first_job_waiting_time",
            color=None if color_option == "none" else color_option,
            labels={
                "first_job_waiting_time": "Median Waiting Time (min)",
                "job_type": "Job Type",
            },
            text=medians["first_job_waiting_time"].apply(lambda x: f"{x:.1f}" if x != 0 else "0")
        )

        # Adjust visuals
        fig.update_traces(textposition='outside', textfont_size=12)

        # Add padding to Y-axis so text isn't cut off
        fig.update_layout(
            yaxis=dict(range=[0, medians["first_job_waiting_time"].max() * 1.15])
        )

        return fig


    
    
    @render_plotly
    def job_waiting_time_by_date():
        if "selected_navset_bar" in input and input.selected_navset_bar() != "All Jobs":
            return None
        data = dataset_data()
        if data.empty:
            return go.Figure()

        df = data.copy()

        # Construct the 'submit_date' from year, month, and day
        df["submit_date"] = pd.to_datetime({
            "year": df["year"],
            "month": df["month"].cat.codes + 1,
            "day": df["day"]
        }, errors="coerce")

        df = df.dropna(subset=["submit_date"])
        df["day"] = df["submit_date"].dt.day

        # Determine whether to display in minutes or hours
        waiting_time_secs = df["first_job_waiting_time"]
        use_minutes = waiting_time_secs.max() <= 5400  # 90 minutes

        if use_minutes:
            df["job_waiting_time_display"] = waiting_time_secs / 60.0
            y_label = "Job Waiting Time (min)"
        else:
            df["job_waiting_time_display"] = waiting_time_secs / 3600.0
            y_label = "Job Waiting Time (hour)"

        # Downsample to 3500 max points
        max_points = 3500
        if len(df) > max_points:
            df = df.sample(n=max_points, random_state=42)

        year, month, _ = selected_year_month()

        fig = px.box(
            df,
            x="day",
            y="job_waiting_time_display",
            color="year",
            labels={
                "day": "Day of Month",
                "job_waiting_time_display": y_label,
                "job_number": "Job Number"
            },
            category_orders={"day": list(range(1, 32))},
            hover_data=["job_number"]
        )

        # Show all individual outlier points
        fig.update_traces(
            boxpoints="outliers",  # ensures outliers are plotted
            marker=dict(size=5, opacity=0.6, line=dict(width=1, color="white")),
            jitter=0.3,
            pointpos=0
        )

        fig.update_layout(
            title={
                "text": f"{year} {month}",
                "x": 0.5,
                "xanchor": "center"
            },
            boxmode="group",
            showlegend=False,
            xaxis=dict(
                tickmode="linear",
                dtick=1,
                tickangle=0
            )
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


    # "Select All" & "Unselect All" handlers
    @reactive.effect
    @reactive.event(input.select_all)
    def _():
        all_job_types = list(dataset["job_type"].unique())
        ui.update_checkbox_group("job_type", selected=all_job_types)

    @reactive.effect
    @reactive.event(input.unselect_all)
    def _():
        ui.update_checkbox_group("job_type", selected=[])

    @output(id=f"{PAGE_ID}_warning_message")
    @render.ui
    def warning_message():
        _, _, warning = selected_year_month()
        if warning:
            return ui.markdown(f"**⚠️ Warning:** {warning}")
        return None

    @reactive.effect
    def sync_year():
        selected_year.set(input.selected_year())

    @reactive.effect
    def sync_month():
        selected_month.set(input.selected_month())