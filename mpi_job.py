import faicons as fa
import pandas as pd
from shiny import ui, render, reactive
from shinywidgets import output_widget, render_plotly
import plotly.express as px
import plotly.graph_objects as go
import datetime

# ----------------------------------------------------------------
# DATA LOADING & PREP
# ----------------------------------------------------------------
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

# ----------------------------------------------------------------
# UI FOR THE MPI JOB PAGE
# ----------------------------------------------------------------

def mpi_job_ui():
    """
    Builds the UI for the MPI Job page,
    including year checkboxes, summary value boxes, and multiple plots.
    """
    return ui.page_fluid(
        ui.output_ui("mpi_warning_message"),
        ui.div(
            ui.div(
                ui.input_text(
                    "selected_year_mpi",
                    "Enter Year",
                    value=str(now.year),
                    placeholder="e.g., 2024"
                ),
                style="margin-right: 20px; width: 250px;"
            ),
            ui.div(
                ui.input_text(
                    "selected_month_mpi",
                    "Enter Month (e.g., Jan, Feb)",
                    value=now.strftime("%b"),
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
                    "Total Waiting Time by CPU Cores",
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
            df = df[df["queue_type"] == "shared"]
        elif queue_filter == "buyin":
            df = df[df["queue_type"] == "buyin"]

        return df


    # SUMMARY STATS (MIN, MAX, MEAN, MEDIAN, COUNT)
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
        # Convert sec -> min and filter negatives
        df_plot["first_job_waiting_time"] = (df_plot["first_job_waiting_time"] / 60).round(2)
        df_plot = df_plot[df_plot["first_job_waiting_time"] >= 0]

        # Group by job_type, compute median
        grouped = df_plot.groupby("job_type")["first_job_waiting_time"].median().reset_index()
        # sort the values in ascending order    
        grouped = grouped.sort_values(by="first_job_waiting_time", ascending=True)
        # Create base figure
        fig = go.Figure()

        # Add visible bar trace
        fig.add_trace(go.Bar(
            x=grouped["job_type"],
            y=grouped["first_job_waiting_time"],
            marker=dict(color='royalblue'),  # Set bar color
            showlegend=False
        ))

        # Configure layout
        fig.update_layout(
            yaxis=dict(
                title="Median Waiting Time (min)",
                range=[0, max(grouped["first_job_waiting_time"].max() * 1.1, 1)],  # Ensure min 0-1 range
                zeroline=True,
                zerolinewidth=2,
            ),
            xaxis=dict(title="Job Type"),
            hovermode="x"
        )

        return fig

    # ---- (B) Box Plot: Job Waiting Time by Month & Year ----
    @render_plotly
    def job_waiting_time_by_month():
        """
        Create a line plot showing the median job waiting time (hours) across the year through 12 months.
        The top 7 queues with the largest total waiting times are displayed individually,
        while the remaining queues are combined into an 'other' category (median of their waiting times).
        """
        df = dataset_data()
        if df.empty:
            print("No data available for Job Waiting Time by Month")
            return go.Figure()

        df_plot = df.copy()

        # Convert sec -> hours
        df_plot["job_waiting_time (hours)"] = df_plot["first_job_waiting_time"] / 3600.0

        # Filter by selected years
        selected_years = list(map(int, input.years()))
        df_plot = df_plot[df_plot['year'].isin(selected_years)]

        # Aggregate total waiting time by queue and year
        queue_totals = (
            df_plot.groupby("job_type")["job_waiting_time (hours)"]
            .sum()
            .reset_index()
        )

        # Identify the top 7 queues with the largest total waiting times
        top_queues = queue_totals.nlargest(7, "job_waiting_time (hours)")["job_type"].tolist()

        # Combine the remaining queues into 'other'
        df_plot["job_type"] = df_plot["job_type"].apply(
            lambda x: x if x in top_queues else "other"
        )

        # Aggregate median waiting time by month, year, and job_type
        df_aggregated = (
            df_plot.groupby(["year", "month", "job_type"], as_index=False)
            ["job_waiting_time (hours)"]
            .median()
        )

        # Create the line plot
        fig = px.line(
            df_aggregated,
            x="month",
            y="job_waiting_time (hours)",
            color="job_type",
            line_dash="year",  # Different line styles for each year
            labels={"job_waiting_time (hours)": "Median Job Waiting Time (hours)"},
            title="Median Job Waiting Time by Month (Top 7 Queues + Other)"
        )

        # Update layout
        fig.update_layout(
            xaxis=dict(title="Month", tickmode="array", tickvals=list(range(1, 13)), ticktext=[str(i) for i in range(1, 13)]),
            yaxis=dict(title="Median Job Waiting Time (hours)"),
            showlegend=True,
            hovermode="x unified"
        )

        return fig


    # ---- (C) Box Plot: Job Waiting Time by CPU Cores ----
    @render_plotly
    def job_waiting_time_by_cpu():
        """
        Create a box plot of job waiting time (hours) grouped into 10 CPU core ranges,
        with outliers preserved during downsampling.
        """
        df = dataset_data()
        if df.empty:
            return go.Figure()

        df_plot = df.copy()

        # Convert waiting time to hours
        df_plot["job_waiting_time (hours)"] = df_plot["first_job_waiting_time"] / 3600.0

        # Ensure 'slots' column is an integer
        df_plot["slots"] = df_plot["slots"].astype(int)

        # Calculate dynamic ranges for 10 groups
        min_core = df_plot["slots"].min()
        max_core = df_plot["slots"].max()
        group_size = (max_core - min_core) // 10

        def group_cpu_cores(slots):
            for i in range(10):
                lower = min_core + i * group_size
                upper = lower + group_size
                if i == 9:  # Last group includes the maximum
                    upper = max_core + 1
                if lower <= slots < upper:
                    return f"{lower}-{upper - 1}"
            return "other"

        df_plot["cpu_group"] = df_plot["slots"].apply(group_cpu_cores)

        # Filter by selected years
        selected_years = list(map(int, input.years()))
        df_plot = df_plot[df_plot['year'].isin(selected_years)]

        # Downsampling logic with outlier preservation
        max_points = 4000  # Set the maximum number of data points
        points_per_year = max_points // len(selected_years) if selected_years else max_points

        # Identify outliers using IQR
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

        df_plot = pd.concat([pd.concat(downsampled_non_outliers), outliers])

        # Ensure consistent ordering for CPU groups
        def safe_sort_key(group):
            try:
                return int(group.split("-")[0])
            except ValueError:
                return float("inf")

        unique_groups = sorted(df_plot["cpu_group"].unique(), key=safe_sort_key)
        df_plot["cpu_group"] = pd.Categorical(df_plot["cpu_group"], categories=unique_groups, ordered=True)

        # Create the box plot
        fig = px.box(
            df_plot,
            x="cpu_group",
            y="job_waiting_time (hours)",
            color="year",
            labels={
                "cpu_group": "CPU Group",
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
