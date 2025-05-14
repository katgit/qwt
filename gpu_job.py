import faicons as fa
import pandas as pd
from shiny import ui, render, reactive
from shinywidgets import output_widget, render_plotly
import plotly.express as px
import plotly.graph_objects as go  # For empty Figure
import datetime
now = datetime.datetime.now()

# DATA LOADING & PREP
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

# UI
PAGE_ID = "gpu_job"
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

def gpu_job_ui(selected_year, selected_month):
    return ui.page_fluid(
        # ------------------ Year Selection ------------------
        ui.output_ui("gpu_warning_message"),
        ui.div(
            ui.div(
                ui.input_text(
                    "selected_year_gpu",
                    "Enter Year",
                    value=selected_year.get(),
                    placeholder="e.g., 2024"
                ),
                style="margin-right: 20px; width: 250px;"
            ),
            ui.div(
                ui.input_text(
                    "selected_month_gpu",
                    "Enter Month (e.g., Jan, Feb)",
                    value=selected_month.get(),
                    placeholder="e.g., Jan"
                ),
                style="margin-right: 20px; width: 250px;"
            ),
            ui.div(
                ui.input_select(
                    "queue_filter_gpu",
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
            style="display: flex; align-items: flex-end; margin-bottom: 1em; margin-top: 1em;"
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
        # ------------------ Value Boxes ---------------------
        ui.layout_columns(
            value_box_custom("Min Waiting Time", f"{PAGE_ID}_min_waiting_time", ICONS["min"]),
            value_box_custom("Max Waiting Time", f"{PAGE_ID}_max_waiting_time", ICONS["max"]),
            value_box_custom("Mean Waiting Time", f"{PAGE_ID}_mean_waiting_time", ICONS["speed"]),
            value_box_custom("Median Waiting Time", f"{PAGE_ID}_median_waiting_time", ICONS["median"]),
            value_box_custom("Number of Jobs", f"{PAGE_ID}_job_count", ICONS["count"]),
            fill=False,
        ),

        # ------------------ Main Content ---------------------
        ui.layout_columns(
            ui.card(
                ui.card_header(
                    "Waiting Time vs Queue",
                    ui.popover(
                        ICONS["ellipsis"],
                        ui.input_radio_buttons(
                            "gpu_scatter_color",
                            None,
                            ["job_type", "none"],
                            inline=True,
                        ),
                        title="Add a color variable",
                        placement="top",
                    ),
                    class_="d-flex justify-content-between align-items-center",
                ),
                output_widget("GPU_barplot"),
                full_screen=True
            ),
            ui.card(
                ui.card_header(
                    "Daily Median Waiting Time",
                    class_="d-flex justify-content-between align-items-center"
                ),
                output_widget("gpu_job_waiting_time_by_month"),
                full_screen=True
            ),
        ),
        fillable=True,
    )

# SERVER LOGIC
def gpu_job_server(input, output, session, selected_year, selected_month):
    """
    Server logic for GPU Job page:
      1) Filter dataset by selected years (and optionally job_type if needed).
      2) Compute summary stats (min, max, mean, median, count).
      3) Render data table and various plots (barplot, boxplot).
    """

    # ------------------ Reactive Filter ------------------
    @reactive.Calc
    def gpu_data():
        try:
            year = int(input.selected_year_gpu())
        except ValueError:
            return dataset.iloc[0:0]

        month = input.selected_month_gpu().capitalize()
        if month not in month_order:
            return dataset.iloc[0:0]

        df = dataset[(dataset["year"] == year) & (dataset["month"] == month)]

        queue_filter = input.queue_filter_gpu()
        if queue_filter == "shared":
            df = df[df["class_own"] == "shared"]
        elif queue_filter == "buyin":
            df = df[(df["class_own"] == "buyin") & (df["class_user"] == "buyin")]

        return df

    # ------------------ Summary Stats ------------------
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

    # ------------------ Value Box Renderers ------------------
    @output(id=f"{PAGE_ID}_min_waiting_time")
    @render.text
    def min_waiting_time():
        stats = gpu_waiting_time_stats()
        if stats["min"] is None:
            return "No data available"
        return f"{stats['min'] / 60:.1f} hours" if stats["min"] > 60 else f"{stats['min']:.1f} min"

    @output(id=f"{PAGE_ID}_max_waiting_time")
    @render.text
    def max_waiting_time():
        stats = gpu_waiting_time_stats()
        if stats["max"] is None:
            return "No data available"
        return f"{stats['max'] / 60:.1f} hours" if stats["max"] > 60 else f"{stats['max']:.1f} min"

    @output(id=f"{PAGE_ID}_mean_waiting_time")
    @render.text
    def mean_waiting_time():
        stats = gpu_waiting_time_stats()
        if stats["mean"] is None:
            return "No data available"
        return f"{stats['mean'] / 60:.1f} hours" if stats["mean"] > 60 else f"{stats['mean']:.1f} min"

    @output(id=f"{PAGE_ID}_median_waiting_time")
    @render.text
    def median_waiting_time():
        stats = gpu_waiting_time_stats()
        if stats["median"] is None:
            return "No data available"
        return f"{stats['median'] / 60:.1f} hours" if stats["median"] > 60 else f"{stats['median']:.1f} min"

    @output(id=f"{PAGE_ID}_job_count")
    @render.text
    def job_count():
        stats = gpu_waiting_time_stats()
        return str(stats["count"])


    # ------------------ Plots ------------------
    @output(id="GPU_barplot")
    @render_plotly
    def GPU_barplot():
        if "selected_navset_bar" in input and input.selected_navset_bar() != "GPU Job":
            return None
        df = gpu_data()
        if df.empty:
            return go.Figure()

        # Filter out invalid times and convert sec -> min
        df = df[df["first_job_waiting_time"] >= 0].copy()
        df["first_job_waiting_time"] = df["first_job_waiting_time"] / 60  # Now in minutes

        # Compute median waiting time
        medians = df.groupby("job_type")["first_job_waiting_time"].median().reset_index()

        # Get top 5 job_types with highest median
        top5 = medians.nlargest(5, "first_job_waiting_time")["job_type"].tolist()

        # Reassign job_type: keep top 5 as-is, label others as 'others'
        df["job_type_grouped"] = df["job_type"].apply(lambda x: x if x in top5 else "others")

        # Group again using the new column
        grouped = (
            df.groupby("job_type_grouped")["first_job_waiting_time"]
            .median()
            .reset_index()
            .sort_values(by="first_job_waiting_time", ascending=True)
        )

        # Determine whether to use min or hr
        convert_to_hours = grouped["first_job_waiting_time"].max() > 100
        unit = "hr" if convert_to_hours else "min"
        grouped["waiting_time_display"] = grouped["first_job_waiting_time"].apply(
            lambda x: round(x / 60, 1) if convert_to_hours else round(x, 1)
        )
        y_values = grouped["waiting_time_display"]

        # Plotting
        color_var = input.gpu_scatter_color()
        fig = px.bar(
            grouped,
            x="job_type_grouped",
            y=y_values,
            color=None if color_var == "none" else "job_type_grouped",
            labels={
                "job_type_grouped": "Job Type",
                "waiting_time_display": f"Median Waiting Time ({unit})"
            },
            text=[f"{val} {unit}" for val in y_values]
        )

        # Layout & style
        fig.update_layout(
            xaxis_title="Queue Type",
            yaxis_title=f"Median Waiting Time ({unit})",
            yaxis=dict(range=[0, max(y_values.max() * 1.1, 1)]),
            showlegend=(color_var != "none"),
            uniformtext_minsize=8,
            uniformtext_mode='hide'
        )

        return fig


    @output(id="gpu_job_waiting_time_by_month")
    @render_plotly
    def gpu_job_waiting_time_by_month():
        """
        Line plot of median job waiting time (hours) per day of the selected month,
        comparing 'GPU = 1' vs 'GPU > 1'.
        """
        if "selected_navset_bar" in input and input.selected_navset_bar() != "GPU Job":
            return None
        df = gpu_data()
        if df.empty or "day" not in df.columns:
            return go.Figure()

        df_plot = df.copy()

        # Clean and convert 'day' column
        df_plot["day"] = pd.to_numeric(df_plot["day"], errors="coerce")
        df_plot.dropna(subset=["day", "first_job_waiting_time", "job_type"], inplace=True)
        df_plot["day"] = df_plot["day"].astype(int)

        # Convert waiting time to hours
        df_plot["job_waiting_time (min)"] = df_plot["first_job_waiting_time"] / 60

        # Simplify job_type into categories
        df_plot["job_type"] = df_plot["job_type"].apply(
            lambda x: "GPU = 1" if str(x).startswith("GPU = 1") else ("GPU > 1" if str(x).startswith("GPU") else x)
        )

        # Group by day and job type, then compute median
        grouped = (
            df_plot.groupby(["day", "job_type"], observed=True)["job_waiting_time (min)"]
            .median()
            .reset_index()
            .sort_values("day")
        )

        # Extract selected year/month for title
        try:
            year = int(input.selected_year_gpu())
            month = input.selected_month_gpu().capitalize()
        except:
            year, month = None, None

        # plot
        fig = px.line(
            grouped,
            x="day",
            y="job_waiting_time (min)",
            color="job_type",
            markers=True,
            title=f"{month} {year}",
            labels={
                "day": "Day of Month",
                "job_waiting_time (min)": "Median Waiting Time (min)",
                "job_type": "GPU Job Type"
            }
        )

        fig.update_layout(
            xaxis=dict(tickmode="linear", dtick=1),
            title={"x": 0.5, "xanchor": "center"},
            hovermode="x unified"
        )

        return fig


    @reactive.effect
    def sync_year():
        selected_year.set(input.selected_year_gpu())

    @reactive.effect
    def sync_month():
        selected_month.set(input.selected_month_gpu())



    @output
    @render.ui
    def gpu_warning_message():
        try:
            year = int(input.selected_year_gpu())
            month = input.selected_month_gpu().capitalize()
        except:
            return ui.markdown("⚠️ Invalid year/month input.")

        if month not in month_order:
            return ui.markdown("⚠️ Invalid month format. Use 3-letter month (e.g., Jan, Feb).")

        if dataset[(dataset["year"] == year) & (dataset["month"] == month)].empty:
            return ui.markdown("⚠️ No data available for this year and month.")

        return None


