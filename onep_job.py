import faicons as fa
import pandas as pd
from shiny import ui, render, reactive
from shinywidgets import output_widget, render_plotly
import plotly.express as px
import plotly.graph_objects as go
import datetime

# DATA LOADING
dataset = pd.read_feather("/projectnb/rcs-intern/Jiazheng/accounting/ShinyApp_Data_OneP.feather")
now = datetime.datetime.now()

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

# ICONS
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
PAGE_ID = "oneP"
# UI DEFINITION
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

    
def oneP_job_ui(selected_year, selected_month):
    return ui.page_fluid(
        ui.output_ui("onep_warning_message"),
        ui.div(
            ui.div(
                ui.input_text(
                    "selected_year_onep",
                    "Enter Year",
                    value=selected_year.get(),
                    placeholder="e.g., 2024"
                ),
                style="margin-right: 20px; width: 250px;"
            ),
            ui.div(
                ui.input_text(
                    "selected_month_onep",
                    "Enter Month (e.g., Jan, Feb)",
                    value=selected_month.get(),
                    placeholder="e.g., Jan"
                ),
                style="margin-right: 20px; width: 250px;"
            ),
            ui.div(
                ui.input_select(
                    "queue_filter_onep",
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
                    "Waiting Time vs Queue",
                    class_="d-flex justify-content-between align-items-center"
                ),
                output_widget("oneP_waiting_time_vs_queue"),
                full_screen=True
            ),
            ui.card(
                ui.card_header(
                    "Daily Median Waiting Time",
                    class_="d-flex justify-content-between align-items-center"
                ),
                output_widget("oneP_job_waiting_time_by_day"),
                full_screen=True
            ),
        ),
        fillable=True,
    )

# SERVER LOGIC
def oneP_job_server(input, output, session, selected_year, selected_month):
    @reactive.Calc
    def oneP_filtered_data():
        try:
            year = int(input.selected_year_onep())
        except ValueError:
            return dataset.iloc[0:0]

        month = input.selected_month_onep().capitalize()
        if month not in month_order:
            return dataset.iloc[0:0]

        df = dataset[(dataset["year"] == year) & (dataset["month"] == month)]

        queue_filter = input.queue_filter_onep()
        if queue_filter == "shared":
            df = df[df["class_own"] == "shared"]
        elif queue_filter == "buyin":
            df = df[(df["class_own"] == "buyin") & (df["class_user"] == "buyin")]

        return df


    @reactive.Calc
    def waiting_time_stats():
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

    @output(id=f"{PAGE_ID}_min_waiting_time")
    @render.text
    def min_waiting_time():
        stats = waiting_time_stats()
        if stats["min"] is None:
            return "No data available"
        return f"{stats['min'] / 60:.1f} hours" if stats["min"] > 60 else f"{stats['min']:.1f} min"

    @output(id=f"{PAGE_ID}_max_waiting_time")
    @render.text
    def max_waiting_time():
        stats = waiting_time_stats()
        if stats["max"] is None:
            return "No data available"
        return f"{stats['max'] / 60:.1f} hours" if stats["max"] > 60 else f"{stats['max']:.1f} min"

    @output(id=f"{PAGE_ID}_mean_waiting_time")
    @render.text
    def mean_waiting_time():
        stats = waiting_time_stats()
        if stats["mean"] is None:
            return "No data available"
        return f"{stats['mean'] / 60:.1f} hours" if stats["mean"] > 60 else f"{stats['mean']:.1f} min"

    @output(id=f"{PAGE_ID}_median_waiting_time")
    @render.text
    def median_waiting_time():
        stats = waiting_time_stats()
        if stats["median"] is None:
            return "No data available"
        return f"{stats['median'] / 60:.1f} hours" if stats["median"] > 60 else f"{stats['median']:.1f} min"

    @output(id=f"{PAGE_ID}_job_count")
    @render.text
    def job_count():
        stats = waiting_time_stats()
        return str(stats["count"])

    @render_plotly
    def oneP_waiting_time_vs_queue():
        if input.selected_navset_bar() != "1-p Job":
            return None
        df = oneP_filtered_data()
        if df.empty:
            return go.Figure()

        df_plot = df.copy()

        # Clean job_type name
        df_plot["job_type"] = df_plot["job_type"].str.replace("1-p ", "", regex=False)

        # Convert to minutes
        df_plot["waiting_time_min"] = df_plot["first_job_waiting_time"] / 60

        # Compute median waiting time per job_type
        medians = df_plot.groupby("job_type")["waiting_time_min"].median().reset_index()

        # Identify top 6 job types with highest median
        top6 = medians.nlargest(6, "waiting_time_min")["job_type"].tolist()

        # Group others under "others"
        df_plot["job_type_grouped"] = df_plot["job_type"].apply(lambda x: x if x in top6 else "others")

        # Recalculate medians with grouped data
        grouped = (
            df_plot.groupby("job_type_grouped")["waiting_time_min"]
            .median()
            .reset_index()
            .sort_values(by="waiting_time_min", ascending=True)
        )

        # Decide unit and format
        convert_to_hours = grouped["waiting_time_min"].max() > 100
        unit = "hr" if convert_to_hours else "min"
        grouped["waiting_time_display"] = grouped["waiting_time_min"].apply(
            lambda x: round(x / 60, 1) if convert_to_hours else round(x, 1)
        )
        y_values = grouped["waiting_time_display"]

        # Build the bar plot
        fig = px.bar(
            grouped,
            x="job_type_grouped",
            y=y_values,
            text=[f"{val} {unit}" for val in y_values],
            labels={
                "job_type_grouped": "Job Type",
                "waiting_time_display": f"Median Waiting Time ({unit})"
            },
        )

        fig.update_layout(
            xaxis_title="Queue Type",
            yaxis_title=f"Median Waiting Time ({unit})",
            yaxis=dict(range=[0, max(y_values.max() * 1.1, 1)]),
            uniformtext_minsize=8,
            uniformtext_mode='hide',
            title={"x": 0.5, "xanchor": "center"}
        )

        return fig


    @render_plotly
    def oneP_job_waiting_time_by_day():
        if input.selected_navset_bar() != "1-p Job":
            return None
        df = oneP_filtered_data()
        if df.empty or "day" not in df.columns:
            return go.Figure()

        df_plot = df.copy()
        df_plot["day"] = pd.to_numeric(df_plot["day"], errors="coerce")
        df_plot = df_plot.dropna(subset=["day"])
        df_plot["day"] = df_plot["day"].astype(int)

        # Convert sec → minutes
        df_plot["job_waiting_time (minutes)"] = df_plot["first_job_waiting_time"] / 60.0

        daily = (
            df_plot.groupby("day")["job_waiting_time (minutes)"]
            .median()
            .reset_index()
            .sort_values("day")
        )

        try:
            year = int(input.selected_year_onep())
            month = input.selected_month_onep().capitalize()
        except:
            year, month = "Unknown", "Unknown"

        fig = px.line(
            daily,
            x="day",
            y="job_waiting_time (minutes)",
            markers=True,
            title=f"{month} {year}",
            labels={
                "day": "Day of Month",
                "job_waiting_time (minutes)": "Median Waiting Time (minutes)"
            }
        )

        fig.update_layout(
            xaxis=dict(tickmode="linear", dtick=1),
            title={"x": 0.5, "xanchor": "center"},
            hovermode="x unified"
        )

        return fig


    @output
    @render.ui
    def onep_warning_message():
        try:
            year = int(input.selected_year_onep())
            month = input.selected_month_onep().capitalize()
        except:
            return ui.markdown("⚠️ Invalid year or month input.")

        if month not in month_order:
            return ui.markdown("⚠️ Invalid month format. Use 3-letter month (e.g., Jan, Feb).")

        if dataset[(dataset["year"] == year) & (dataset["month"] == month)].empty:
            return ui.markdown("⚠️ No data available for this year and month.")

        return None

    @reactive.effect
    def sync_year():
        selected_year.set(input.selected_year_onep())

    @reactive.effect
    def sync_month():
        selected_month.set(input.selected_month_onep())