import faicons as fa
import pandas as pd
from shiny import ui, render, reactive
from shinywidgets import output_widget, render_plotly
import plotly.express as px
import plotly.graph_objects as go
import datetime

# DATA LOADING & PREP
now = datetime.datetime.now()
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

# UI FOR THE MPI JOB PAGE
PAGE_ID = "mpi_job"
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


def mpi_job_ui(selected_year, selected_month):
    return ui.page_fluid(
        ui.output_ui("mpi_warning_message"),
        ui.div(
            ui.div(
                ui.input_text(
                    "selected_year_mpi",
                    "Enter Year",
                    value=selected_year.get(),
                    placeholder="e.g., 2024"
                ),
                style="margin-right: 20px; width: 250px;"
            ),
            ui.div(
                ui.input_text(
                    "selected_month_mpi",
                    "Enter Month (e.g., Jan, Feb)",
                    value=selected_month.get(),
                    placeholder="e.g., Jan"
                ),
                style="margin-right: 20px; width: 250px;"
            ),
            ui.div(
                ui.input_select(
                    "queue_filter_mpi",
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
                    ui.popover(
                        ICONS["ellipsis"],
                        ui.input_radio_buttons(
                            "mpi_scatter_color",
                            None,
                            ["job_type", "none"],
                            inline=True,
                        ),
                        title="Add a color variable",
                        placement="top",
                    ),
                    class_="d-flex justify-content-between align-items-center",
                ),
                output_widget("mpi_barplot"),
                full_screen=True
            ),
            ui.card(
                ui.card_header(
                    "Daily Median Waiting Time",
                    class_="d-flex justify-content-between align-items-center"
                ),
                output_widget("mpi_job_waiting_time_by_day"),
                full_screen=True
            ),
            ui.card(
                ui.card_header(
                    "Total Waiting Time by CPU Cores",
                    class_="d-flex justify-content-between align-items-center"
                ),
                output_widget("job_waiting_time_by_cpu"),
                full_screen=True
            ),
            col_widths=[6, 6, 6]
        ),
        fillable=True,
    )

# SERVER LOGIC
def mpi_job_server(input, output, session, selected_year, selected_month):
    print("MPI Job server function called")

    # 1) Reactive data filter by selected years (and possibly more in future)
    @reactive.calc
    def dataset_data():
        try:
            year = int(input.selected_year_mpi())
        except ValueError:
            return dataset.iloc[0:0]

        month = input.selected_month_mpi().capitalize()
        if month not in month_order:
            return dataset.iloc[0:0]

        df = dataset[(dataset["year"] == year) & (dataset["month"] == month)]

        queue_filter = input.queue_filter_mpi()
        if queue_filter == "shared":
            df = df[df["class_own"] == "shared"]
        elif queue_filter == "buyin":
            df = df[(df["class_own"] == "buyin") & (df["class_user"] == "buyin")]

        return df


    # SUMMARY STATS (MIN, MAX, MEAN, MEDIAN, COUNT)
    @reactive.calc
    def stats():
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

    @output(id=f"{PAGE_ID}_min_waiting_time")
    @render.text
    def min_waiting_time():
        s = stats()
        if s["min"] is None:
            return "No data available"
        return f"{s['min'] / 60:.1f} hours" if s["min"] > 60 else f"{s['min']:.1f} min"

    @output(id=f"{PAGE_ID}_max_waiting_time")
    @render.text
    def max_waiting_time():
        s = stats()
        if s["max"] is None:
            return "No data available"
        return f"{s['max'] / 60:.1f} hours" if s["max"] > 60 else f"{s['max']:.1f} min"

    @output(id=f"{PAGE_ID}_mean_waiting_time")
    @render.text
    def mean_waiting_time():
        s = stats()
        if s["mean"] is None:
            return "No data available"
        return f"{s['mean'] / 60:.1f} hours" if s["mean"] > 60 else f"{s['mean']:.1f} min"

    @output(id=f"{PAGE_ID}_median_waiting_time")
    @render.text
    def median_waiting_time():
        s = stats()
        if s["median"] is None:
            return "No data available"
        return f"{s['median'] / 60:.1f} hours" if s["median"] > 60 else f"{s['median']:.1f} min"

    @output(id=f"{PAGE_ID}_job_count")
    @render.text
    def job_count():
        return str(stats()["count"])

    # 4) OPTIONAL DATA GRID (unused in your UI, but here for reference)
    @render.data_frame
    def table():
        df = dataset_data()
        if df.empty:
            return pd.DataFrame()

        df_mod = df.copy()
        df_mod["first_job_waiting_time"] = (df_mod["first_job_waiting_time"] / 60).round(2)
        df_mod.rename(columns={"first_job_waiting_time": "first_job_waiting_time (min)"}, inplace=True)
        return render.DataGrid(df_mod)

    # 5) PLOTS

    #Bar Plot: Median waiting time by job_type ----
    @render_plotly
    def mpi_barplot():
        if "selected_navset_bar" in input and input.selected_navset_bar() != "MPI Job":
            return None
        df = dataset_data()
        if df.empty:
            print("No data available for bar plot in MPI Job")
            return go.Figure()

        df_plot = df.copy()

        # Remove "MPI job " prefix
        df_plot["job_type"] = df_plot["job_type"].str.replace("MPI job ", "", regex=False)

        # Convert to minutes and filter invalid times
        df_plot["first_job_waiting_time"] = df_plot["first_job_waiting_time"] / 60  # Convert to minutes
        df_plot = df_plot[df_plot["first_job_waiting_time"] >= 0]

        # Compute median waiting time per job_type
        medians = df_plot.groupby("job_type")["first_job_waiting_time"].median().reset_index()

        # Get top 6 job_types with largest medians
        top6 = medians.nlargest(6, "first_job_waiting_time")["job_type"].tolist()

        # Reassign job types into top 6 or 'others'
        df_plot["job_type_grouped"] = df_plot["job_type"].apply(lambda x: x if x in top6 else "others")

        # Recalculate median on grouped data
        grouped = (
            df_plot.groupby("job_type_grouped")["first_job_waiting_time"]
            .median()
            .reset_index()
            .sort_values("first_job_waiting_time", ascending=True)
        )

        # Determine unit: minutes or hours
        convert_to_hours = grouped["first_job_waiting_time"].max() > 100
        unit = "hr" if convert_to_hours else "min"
        grouped["waiting_time_display"] = grouped["first_job_waiting_time"].apply(
            lambda x: round(x / 60, 1) if convert_to_hours else round(x, 1)
        )
        y_values = grouped["waiting_time_display"]

        # Create bar chart with labels
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=grouped["job_type_grouped"],
            y=y_values,
            text=[f"{val} {unit}" for val in y_values],
            textposition="outside",
            marker=dict(color='royalblue'),
            showlegend=False
        ))

        # Layout
        fig.update_layout(
            yaxis=dict(
                title=f"Median Waiting Time ({unit})",
                range=[0, max(y_values.max() * 1.1, 1)],
                zeroline=True,
                zerolinewidth=2,
            ),
            xaxis=dict(title="Queue Type"),
            hovermode="x",
            title={
                "x": 0.5,
                "xanchor": "center"
            },
            uniformtext_minsize=8,
            uniformtext_mode='hide'
        )

        return fig




    # Box Plot: Job Waiting Time by day per month ----
    @render_plotly
    def mpi_job_waiting_time_by_day():
        if "selected_navset_bar" in input and input.selected_navset_bar() != "MPI Job":
            return None
        df = dataset_data()
        if df.empty or "day" not in df.columns:
            return go.Figure()

        df_plot = df.copy()
        df_plot["day"] = pd.to_numeric(df_plot["day"], errors="coerce")
        df_plot = df_plot.dropna(subset=["day"])
        df_plot["day"] = df_plot["day"].astype(int)

        # Convert sec -> minutes
        df_plot["job_waiting_time (minutes)"] = df_plot["first_job_waiting_time"] / 60.0

        # Aggregate by day
        daily_median = (
            df_plot.groupby("day")["job_waiting_time (minutes)"]
            .median()
            .reset_index()
            .sort_values("day")
        )

        # Get current month/year for title
        try:
            year = int(input.selected_year_mpi())
            month = input.selected_month_mpi().capitalize()
        except:
            year, month = None, None

        fig = px.line(
            daily_median,
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




    # Box Plot: Job Waiting Time by CPU Cores ----
    @render_plotly
    def job_waiting_time_by_cpu():
        if "selected_navset_bar" in input and input.selected_navset_bar() != "MPI Job":
            return None
        df = dataset_data()
        if df.empty:
            return go.Figure()

        df_plot = df.copy()
        df_plot["job_waiting_time (hours)"] = df_plot["first_job_waiting_time"] / 3600.0
        df_plot["slots"] = df_plot["slots"].astype(int)

        try:
            selected_years = list(map(int, input.years()))
        except:
            selected_years = sorted(df_plot["year"].unique())

        df_plot = df_plot[df_plot["year"].isin(selected_years)]

        min_core = df_plot["slots"].min()
        max_core = df_plot["slots"].max()
        range_span = max(max_core - min_core, 1)
        group_size = max(range_span // 10, 1)

        def group_cpu_cores(slots):
            for i in range(10):
                lower = min_core + i * group_size
                upper = lower + group_size
                if i == 9:
                    upper = max_core + 1
                if lower <= slots < upper:
                    return f"{lower}-{upper - 1}"
            return "other"

        df_plot["cpu_group"] = df_plot["slots"].apply(group_cpu_cores)

        max_points = 4000
        points_per_year = max_points // len(selected_years) if selected_years else max_points

        Q1 = df_plot["job_waiting_time (hours)"].quantile(0.25)
        Q3 = df_plot["job_waiting_time (hours)"].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        outliers = df_plot[(df_plot["job_waiting_time (hours)"] < lower_bound) |
                        (df_plot["job_waiting_time (hours)"] > upper_bound)]
        non_outliers = df_plot[(df_plot["job_waiting_time (hours)"] >= lower_bound) &
                            (df_plot["job_waiting_time (hours)"] <= upper_bound)]

        downsampled_non_outliers = []
        for year in selected_years:
            year_data = non_outliers[non_outliers['year'] == year]
            if len(year_data) > points_per_year:
                year_data = year_data.sample(n=points_per_year, random_state=42)
            downsampled_non_outliers.append(year_data)

        downsampled_df = pd.concat(downsampled_non_outliers + [outliers], ignore_index=True)

        def safe_sort_key(group):
            try:
                return int(group.split("-")[0])
            except:
                return float("inf")

        unique_groups = sorted(downsampled_df["cpu_group"].unique(), key=safe_sort_key)
        downsampled_df["cpu_group"] = pd.Categorical(
            downsampled_df["cpu_group"], categories=unique_groups, ordered=True
        )

        fig = px.box(
            downsampled_df,
            x="cpu_group",
            y="job_waiting_time (hours)",
            color="year",
            labels={
                "cpu_group": "CPU Core Group",
                "job_waiting_time (hours)": "Job Waiting Time (hours)"
            }
        )

        fig.update_layout(
            title={
                "x": 0.5,
                "xanchor": "center"
            },
            yaxis_title="Job Waiting Time (hours)",
            xaxis_title="CPU Core Group",
            boxmode="group",
            showlegend=False
        )
        fig.update_yaxes(rangemode="tozero")



        fig.update_traces(
            marker=dict(size=6, opacity=0.6, line=dict(width=1, color="white")),
            boxpoints="outliers",
            jitter=0.3,
            pointpos=0
        )

        return fig



    @reactive.effect
    def sync_year():
        selected_year.set(input.selected_year_mpi())

    @reactive.effect
    def sync_month():
        selected_month.set(input.selected_month_mpi())

    @output
    @render.ui
    def mpi_warning_message():
        try:
            year = int(input.selected_year_mpi())
            month = input.selected_month_mpi().capitalize()
        except:
            return ui.markdown("⚠️ Invalid year or month input.")

        if month not in month_order:
            return ui.markdown("⚠️ Invalid month format. Use 3-letter month (e.g., Jan, Feb).")

        if dataset[(dataset["year"] == year) & (dataset["month"] == month)].empty:
            return ui.markdown("⚠️ No data available for this year and month.")

        return None
