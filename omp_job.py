import faicons as fa
import pandas as pd
from shiny import ui, render, reactive
from shinywidgets import output_widget, render_plotly
import plotly.express as px
import plotly.graph_objects as go
from sklearn.cluster import KMeans
import datetime


# Load data from Feather
dataset = pd.read_feather("/projectnb/rcs-intern/Jiazheng/accounting/ShinyApp_Data_OMP.feather")
now = datetime.datetime.now()

# Drop rows with any NaN values (if desired, specify subset= for selective dropping)
dataset.dropna(inplace=True)

# Convert 'year' column to integer type
dataset['year'] = dataset['year'].astype(int)

# Set the month column to a categorical with a fixed order
month_order = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
]
dataset["month"] = pd.Categorical(
    dataset["month"], 
    categories=month_order, 
    ordered=True
)

# Identify the CPU cores (slots) used by OMP jobs
cpus = sorted(dataset[dataset.job_type == 'omp'].slots.unique().tolist())

# Define CPU ranges (groupings)
cpu_ranges = {
    "2-4":   [2, 3, 4],
    "5-8":   [5, 6, 7, 8],
    "6-15":  [6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
    "16":    [16],
    "17-27": [17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27],
    "28":    [28],
    "32":    [32],
    "36":    [36],
    "other": []
}

# Any CPUs above 36 go in the "other" bin
cpu_ranges["other"].extend([int(cpu) for cpu in cpus if cpu > 36])

# Define the set of icons
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

# HELPER FUNCTIONS
PAGE_ID = "omp_job"
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


def get_expanded_cpu_selection(cpus_selected):
    if not cpus_selected:
        return []
    # Collect CPU cores from the cpu_ranges dictionary
    selected_values = [cpu_ranges.get(label, []) for label in cpus_selected]
    # Flatten the nested lists into a set to remove duplicates
    expanded_cpus_selected = {val for sublist in selected_values for val in sublist}
    # Convert back to sorted list for consistency
    return sorted(expanded_cpus_selected)

def label_cpu_group(slot):
    for key, cpu_list in cpu_ranges.items():
        if slot in cpu_list:
            return key
    return "other"

# UI COMPONENT
def omp_job_ui(selected_year, selected_month):
    return ui.page_fluid(
        ui.output_ui("omp_warning_message"),
        ui.div(
            ui.div(
                ui.input_text(
                    "selected_year_omp",
                    "Enter Year",
                    value=selected_year.get(),
                    placeholder="e.g., 2024"
                ),
                style="margin-right: 20px; width: 250px;"
            ),
            ui.div(
                ui.input_text(
                    "selected_month_omp",
                    "Enter Month (e.g., Jan, Feb)",
                    value=selected_month.get(),
                    placeholder="e.g., Jan"
                ),
                style="margin-right: 20px; width: 250px;"
            ),
            ui.div(
                ui.input_select(
                    "queue_filter_omp",
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
        ui.input_checkbox_group(
            "cpus",
            "Select CPU Cores",
            list(cpu_ranges.keys()),  # use the keys of cpu_ranges
            selected=list(cpu_ranges.keys()),            # initially select all available cpus
            inline=True
        ),

        # Buttons to quickly select/unselect all
        ui.row(
            ui.div(
                ui.input_action_button("select_all_cpus", "Select All", class_="btn btn-primary me-2"),
                ui.input_action_button("unselect_all_cpus", "Unselect All", class_="btn btn-secondary"),
                class_="d-flex"
            ),
            class_="mb-3"  # Add margin below the row
        ),

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
                            "omp_scatter_color1",
                            None,
                            ["job_type", "none"],
                            inline=True,
                        ),
                        title="Add a color variable",
                        placement="top",
                    ),
                    class_="d-flex justify-content-between align-items-center",
                ),
                output_widget("OMP_waiting_time_vs_queue"),
                full_screen=True
            ),
            ui.card(
                ui.card_header(
                    "Median Waiting Time by CPU Group",
                    ui.popover(
                        ICONS["ellipsis"],
                        ui.input_radio_buttons(
                            "omp_scatter_color2",
                            None,
                            ["job_type", "none"],
                            inline=True,
                        ),
                        title="Add a color variable",
                        placement="top",
                    ),
                    class_="d-flex justify-content-between align-items-center",
                ),
                output_widget("omp_barplot"),  # Bar plot of median waiting time by CPU group
                full_screen=True,
                class_="mb-4" # add margin below 
            ),
            ui.card(
                ui.card_header(
                    "Daily Median Waiting Time",
                    class_="d-flex justify-content-between align-items-center"
                ),
                output_widget("omp_job_waiting_time_by_day"),  # Box plot by month/year/cpu_group
                full_screen=True,
                class_="mb-4" # add margin below 
            ),
            col_widths=[6, 6, 6]
        ),
        fillable=True,
    )

# SERVER LOGIC
def omp_job_server(input, output, session, selected_year, selected_month):
    print("OMP Job server function called")

    @reactive.calc
    def dataset_data():
        try:
            year = int(input.selected_year_omp())
        except ValueError:
            return dataset.iloc[0:0]

        month = input.selected_month_omp().capitalize()
        if month not in month_order:
            return dataset.iloc[0:0]

        queue_filter = input.queue_filter_omp()
        cpus_selected = input.cpus()
        expanded_cpus_selected = get_expanded_cpu_selection(cpus_selected)

        df = dataset[
            (dataset["year"] == year) &
            (dataset["month"] == month) &
            (dataset["slots"].isin(expanded_cpus_selected))
        ]

        if queue_filter == "shared":
            df = df[df["class_own"] == "shared"]
        elif queue_filter == "buyin":
            df = df[(df["class_own"] == "buyin") & (df["class_user"] == "buyin")]

        return df


    @output(id=f"{PAGE_ID}_min_waiting_time")
    @render.text
    def min_waiting_time():
        data = dataset_data()
        if data.empty:
            return "No data available"

        min_wt = max(data.first_job_waiting_time.min() / 60, 0)  # convert sec -> min
        return f"{min_wt / 60:.1f} hours" if min_wt > 60 else f"{min_wt:.1f} min"

    @output(id=f"{PAGE_ID}_max_waiting_time")
    @render.text
    def max_waiting_time():
        data = dataset_data()
        if data.empty:
            return "No data available"

        max_wt = data.first_job_waiting_time.max() / 60  # convert sec -> min
        return f"{max_wt / 60:.1f} hours" if max_wt > 60 else f"{max_wt:.1f} min"

    @output(id=f"{PAGE_ID}_mean_waiting_time")
    @render.text
    def mean_waiting_time():
        data = dataset_data()
        if data.empty:
            return "No data available"

        mean_wt = data.first_job_waiting_time.mean() / 60  # convert sec -> min
        return f"{mean_wt / 60:.1f} hours" if mean_wt > 60 else f"{mean_wt:.1f} min"

    @output(id=f"{PAGE_ID}_median_waiting_time")
    @render.text
    def median_waiting_time():
        data = dataset_data()
        if data.empty:
            return "No data available"

        med_wt = data.first_job_waiting_time.median() / 60  # convert sec -> min
        return f"{med_wt / 60:.1f} hours" if med_wt > 60 else f"{med_wt:.1f} min"

    @output(id=f"{PAGE_ID}_job_count")
    @render.text
    def job_count():
        data = dataset_data()
        return f"{data.shape[0]}"


    # this is table frame. Not currently in use, but could be useful in future
    @render.data_frame
    def table():
        df = dataset_data().copy()
        df['first_job_waiting_time'] = (df['first_job_waiting_time'] / 60).round(2)
        df.rename(columns={'first_job_waiting_time': 'first_job_waiting_time (min)'}, 
                  inplace=True)
        return render.DataGrid(df)


    @render_plotly
    def OMP_waiting_time_vs_queue():
        if input.selected_navset_bar() != "OMP Job":
            return None
        data = dataset_data().copy()
        if data.empty:
            print("No data available for bar plot in OMP Job")
            return go.Figure()

        df_plot = data.copy()

        # Clean job_type name
        df_plot["job_type"] = df_plot["job_type"].str.replace("OMP ", "", regex=False)

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
    def omp_barplot():
        if input.selected_navset_bar() != "OMP Job":
            return None
        data = dataset_data().copy()
        if data.empty:
            print("No data available for bar plot in OMP Job")
            return go.Figure()

        # Convert from seconds to minutes and filter out negatives
        data = data[data["first_job_waiting_time"] >= 0]
        data['first_job_waiting_time'] = data['first_job_waiting_time'] / 60  # Now in minutes

        # Create a CPU group column
        data['cpu_group'] = data['slots'].apply(label_cpu_group)

        # Group by 'cpu_group' and compute median waiting time
        grouped = data.groupby("cpu_group")["first_job_waiting_time"].median().reset_index()

        # Preserve order of CPU ranges
        group_order = list(cpu_ranges.keys())
        grouped['cpu_group'] = pd.Categorical(grouped['cpu_group'], categories=group_order, ordered=True)
        grouped.sort_values('cpu_group', inplace=True)

        # Determine whether to switch to hours
        convert_to_hours = grouped["first_job_waiting_time"].max() > 100
        unit = "hr" if convert_to_hours else "min"
        grouped["waiting_time_display"] = grouped["first_job_waiting_time"].apply(
            lambda x: round(x / 60, 1) if convert_to_hours else round(x, 1)
        )
        y_values = grouped["waiting_time_display"]

        # Build the bar plot
        fig = px.bar(
            grouped,
            x="cpu_group",
            y=y_values,
            labels={
                "cpu_group": "CPU Groups",
                "waiting_time_display": f"Median Waiting Time ({unit})"
            },
            text=[f"{val} {unit}" for val in y_values]
        )

        # Final layout updates
        fig.update_layout(
            xaxis_title="CPU Groups",
            yaxis_title=f"Median Waiting Time ({unit})",
            yaxis=dict(range=[0, max(y_values.max() * 1.1, 1)]),
            uniformtext_minsize=8,
            uniformtext_mode='hide',
            title={
                "x": 0.5,
                "xanchor": "center"
            }
        )

        return fig


    @render_plotly
    def omp_job_waiting_time_by_day():
        if input.selected_navset_bar() != "OMP Job":
            return None
        df = dataset_data()
        if df.empty or "day" not in df.columns:
            return go.Figure()

        df_plot = df.copy()

        # Ensure 'day' is numeric
        df_plot["day"] = pd.to_numeric(df_plot["day"], errors="coerce")
        df_plot = df_plot.dropna(subset=["day"])
        df_plot["day"] = df_plot["day"].astype(int)

        # Convert sec -> minutes
        df_plot["job_waiting_time (minutes)"] = df_plot["first_job_waiting_time"] / 60.0

        # Group by day
        daily = (
            df_plot.groupby("day")["job_waiting_time (minutes)"]
            .median()
            .reset_index()
            .sort_values("day")
        )

        # Dynamic title
        try:
            year = int(input.selected_year_omp())
            month = input.selected_month_omp().capitalize()
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
            title={"x": 0.5, "xanchor": "center"},
            xaxis=dict(tickmode="linear", dtick=1),
            hovermode="x unified"
        )

        return fig






    @reactive.effect
    @reactive.event(input.select_all)
    def _():
        omp_jobs = [job for job in dataset.job_type.unique() if "OMP" in job]
        ui.update_checkbox_group("job_type", selected=omp_jobs)

    @reactive.effect
    @reactive.event(input.unselect_all)
    def _():
        ui.update_checkbox_group("job_type", selected=[])

    @reactive.effect
    @reactive.event(input.cpus)
    def _():
        selected_cpus = input.cpus()
        filtered_jobs = dataset[dataset['slots'].isin(get_expanded_cpu_selection(selected_cpus))]
        # Do something with filtered_jobs if needed

    @reactive.effect
    @reactive.event(input.select_all_cpus)
    def _():
        available_cpus = sorted(dataset.slots.unique().tolist())
        selected_labels = []
        # For each label in cpu_ranges, check if at least one CPU in that range is in 'available_cpus'
        for label, cpus_in_range in cpu_ranges.items():
            if any(cpu in available_cpus for cpu in cpus_in_range):
                selected_labels.append(label)
        # Update CPU checkboxes
        ui.update_checkbox_group("cpus", selected=selected_labels)
        # Optionally, select all months
        ui.update_checkbox_group("months", selected=month_order)
        class_="mb-3"

    @reactive.effect
    @reactive.event(input.unselect_all_cpus)
    def _():
        ui.update_checkbox_group("cpus", selected=[])
        ui.update_checkbox_group("months", selected=[])

    # warning message render
    @output
    @render.ui
    def omp_warning_message():
        try:
            year = int(input.selected_year_omp())
            month = input.selected_month_omp().capitalize()
        except:
            return ui.markdown("⚠️ Invalid year or month input.")

        if month not in month_order:
            return ui.markdown("⚠️ Invalid month format. Use 3-letter month (e.g., Jan, Feb).")

        if dataset[(dataset["year"] == year) & (dataset["month"] == month)].empty:
            return ui.markdown("⚠️ No data available for this year and month.")

        return None

    @reactive.effect
    def sync_year():
        selected_year.set(input.selected_year_omp())

    @reactive.effect
    def sync_month():
        selected_month.set(input.selected_month_omp())