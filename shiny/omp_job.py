import faicons as fa
import pandas as pd
from shiny import ui, render, reactive
from shinywidgets import output_widget, render_plotly
import plotly.express as px
import plotly.graph_objects as go
from sklearn.cluster import KMeans

# -------------------------------------------------------------------------
# DATA LOADING & PREPROCESSING
# -------------------------------------------------------------------------

# Load data from Feather
dataset = pd.read_feather("/projectnb/rcs-intern/Jiazheng/accounting/ShinyApp_Data_OMP.feather")

# Drop rows with any NaN values (if desired, specify subset= for selective dropping)
dataset.dropna(inplace=True)

# Convert 'year' column to integer type
dataset['year'] = dataset['year'].astype(int)

# Set the month column to a categorical with a fixed order
month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
               'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
dataset['month'] = pd.Categorical(dataset['month'], 
                                  categories=month_order, 
                                  ordered=True)

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

# -------------------------------------------------------------------------
# HELPER FUNCTIONS
# -------------------------------------------------------------------------

def get_expanded_cpu_selection(cpus_selected):
    """
    Given a list of CPU group labels (keys in cpu_ranges),
    return a list of all individual CPU cores belonging to those groups.
    """
    if not cpus_selected:
        return []
    # Collect CPU cores from the cpu_ranges dictionary
    selected_values = [cpu_ranges.get(label, []) for label in cpus_selected]
    # Flatten the nested lists into a set to remove duplicates
    expanded_cpus_selected = {val for sublist in selected_values for val in sublist}
    # Convert back to sorted list for consistency
    return sorted(expanded_cpus_selected)

def label_cpu_group(slot):
    """
    Given an integer CPU slot, return the corresponding
    CPU group label (e.g., "2-4", "5-8", "other", etc.).
    """
    for key, cpu_list in cpu_ranges.items():
        if slot in cpu_list:
            return key
    return "other"

# -------------------------------------------------------------------------
# UI COMPONENT
# -------------------------------------------------------------------------

def omp_job_ui():
    """
    Build the UI for the OMP Job page.
    Returns a Shiny UI object containing the input controls,
    value boxes, data table, and plots.
    """
    return ui.page_fluid(
        # -------------------- Filters --------------------
        ui.input_checkbox_group(
            "years",
            "Select Year(s)",
            list(range(2013, 2026)),  # from 2013 to 2024
            selected=[2024],          # default selected year
            inline=True
        ),
        ui.input_checkbox_group(
            "months",
            "Select Month(s)",
            month_order,
            selected=['Jan'],
            inline=True
        ),
        ui.input_checkbox_group(
            "cpus",
            "Select CPU Cores",
            list(cpu_ranges.keys()),  # use the keys of cpu_ranges
            selected=cpus,            # initially select all available cpus
            inline=True
        ),

        # Buttons to quickly select/unselect all
        ui.input_action_button("select_all_cpus", "Select All", class_="btn btn-primary"),
        ui.input_action_button("unselect_all_cpus", "Unselect All", class_="btn btn-secondary"),

        # -------------------- Value Boxes --------------------
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

        # -------------------- Main Content (Data Table & Plots) --------------------
        ui.layout_columns(
            # ui.card(
            #     ui.card_header("Dataset Data"),
            #     ui.output_data_frame("displayTable"),  # Display the filtered data
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
                output_widget("barplot"),  # Bar plot of median waiting time by CPU group
                full_screen=True
            ),
            ui.card(
                ui.card_header(
                    "Box Plot of Job Waiting Time by Month & Year",
                    class_="d-flex justify-content-between align-items-center"
                ),
                output_widget("job_waiting_time_by_month"),  # Box plot by month/year/cpu_group
                full_screen=True
            ),
            col_widths=[6, 6, 6]
        ),
        fillable=True,
    )

# -------------------------------------------------------------------------
# SERVER LOGIC
# -------------------------------------------------------------------------

def omp_job_server(input, output, session):
    """
    Server logic for the OMP Job page.

    - Reactive dataset filter based on selected years, months, and CPU cores.
    - Various reactive renderers for data table and Plotly plots.
    - Handlers to select/unselect CPU cores.
    """
    print("OMP Job server function called")

    @reactive.calc
    def dataset_data():
        """
        Reactive expression to filter the global 'dataset' based on
        selected years, months, and CPU cores from the UI.
        """
        print("dataset_data function called for OMP Job")

        # Get selections from the UI
        years = input.years()
        months = input.months()
        cpus_selected = input.cpus()

        # Ensure years are integers
        years = list(map(int, years))

        # Get expanded list of CPU cores from range labels
        expanded_cpus_selected = get_expanded_cpu_selection(cpus_selected)

        # Build filters
        idx_years  = dataset['year'].isin(years)           if years  else False
        idx_months = dataset['month'].isin(months)         if months else False
        idx_cpus   = dataset['slots'].isin(expanded_cpus_selected) \
                     if expanded_cpus_selected else False

        # Filter data
        filtered_data = dataset[idx_years & idx_months & idx_cpus]
        return filtered_data

    # -------------------- Render: Data Table --------------------
    # @output
    # @render.data_frame
    # def displayTable():
    #     """
    #     Display a filtered subset of the main dataset.
    #     Waiting time is shown in minutes (numeric), but as a string for user readability.
    #     Sorted by job_number for clarity.
    #     """
    #     data = dataset_data().copy()
    #     # Convert waiting time to minutes (string-based representation)
    #     data['first_job_waiting_time'] = data['first_job_waiting_time'].apply(
    #         lambda x: f"{x / 60:.1f}"  # e.g. "123.4"
    #     )
    #     # Sort by job_number
    #     data.sort_values(by='job_number', ascending=True, inplace=True)
    #     # Rename columns for display
    #     data_renamed = data.rename(
    #         columns={
    #             'job_type': 'Job Type',
    #             'first_job_waiting_time': 'Waiting Time (min)',
    #             'month': 'Month',
    #             'job_number': 'Job Number',
    #             'year': 'Year',
    #             'slots': 'CPU Cores'
    #         }
    #     )
    #     return data_renamed

    # -------------------- Value Boxes: Summary Stats --------------------

    @render.text
    def min_waiting_time():
        """
        Compute the minimum waiting time for the filtered data (in minutes or hours).
        """
        data = dataset_data()
        if data.empty:
            return "No data available"

        min_wt = max(data.first_job_waiting_time.min() / 60, 0)  # convert sec -> min
        return f"{min_wt / 60:.1f} hours" if min_wt > 60 else f"{min_wt:.1f} min"

    @render.text
    def max_waiting_time():
        """
        Compute the maximum waiting time for the filtered data (in minutes or hours).
        """
        data = dataset_data()
        if data.empty:
            return "No data available"

        max_wt = data.first_job_waiting_time.max() / 60  # convert sec -> min
        return f"{max_wt / 60:.1f} hours" if max_wt > 60 else f"{max_wt:.1f} min"

    @render.text
    def mean_waiting_time():
        """
        Compute the mean waiting time for the filtered data (in minutes or hours).
        """
        data = dataset_data()
        if data.empty:
            return "No data available"

        mean_wt = data.first_job_waiting_time.mean() / 60  # convert sec -> min
        return f"{mean_wt / 60:.1f} hours" if mean_wt > 60 else f"{mean_wt:.1f} min"

    @render.text
    def median_waiting_time():
        """
        Compute the median waiting time for the filtered data (in minutes or hours).
        """
        data = dataset_data()
        if data.empty:
            return "No data available"

        med_wt = data.first_job_waiting_time.median() / 60  # convert sec -> min
        return f"{med_wt / 60:.1f} hours" if med_wt > 60 else f"{med_wt:.1f} min"

    @render.text
    def job_count():
        """
        Compute the total number of jobs in the filtered dataset.
        """
        data = dataset_data()
        return f"{data.shape[0]}"

    # -------------------- Additional Data Frame (not currently used) --------------------

    @render.data_frame
    def table():
        """
        An additional data frame renderer (if you need it). Converts waiting time
        to minutes with 2 decimal places, then returns the DataGrid object.
        """
        df = dataset_data().copy()
        df['first_job_waiting_time'] = (df['first_job_waiting_time'] / 60).round(2)
        df.rename(columns={'first_job_waiting_time': 'first_job_waiting_time (min)'}, 
                  inplace=True)
        return render.DataGrid(df)

    # -------------------- Plot: Bar Plot of Median Waiting Time by CPU Group --------------------
    @render_plotly
    def barplot():
        """
        Create a bar plot showing median waiting time (minutes)
        by CPU group for the filtered dataset.
        """
        data = dataset_data().copy()
        if data.empty:
            print("No data available for bar plot in OMP Job")
            return go.Figure()

        # Convert from seconds to minutes
        data = data[data["first_job_waiting_time"] >= 0]
        data['first_job_waiting_time'] = (data['first_job_waiting_time'] / 60).round(2)

        # Create a CPU group column
        data['cpu_group'] = data['slots'].apply(label_cpu_group)

        # Group by 'cpu_group' and compute median waiting time
        grouped = data.groupby("cpu_group")["first_job_waiting_time"].median().reset_index()

        # Sort the groups to maintain the CPU range order
        group_order = list(cpu_ranges.keys())
        grouped['cpu_group'] = pd.Categorical(grouped['cpu_group'],
                                              categories=group_order,
                                              ordered=True)
        grouped.sort_values('cpu_group', inplace=True)

        # Build the bar plot
        fig = px.bar(
            grouped,
            x="cpu_group",
            y="first_job_waiting_time",
            labels={
                "first_job_waiting_time": "Median Waiting Time (min)",
                "cpu_group": "CPU Groups"
            },
            text_auto='.1f'
        )
        return fig

    # -------------------- Plot: Box Plot of Job Waiting Time by Month & Year --------------------
    @render_plotly
    def job_waiting_time_by_month():
        """
        Create a box plot (facet by CPU group) showing job waiting time (hours)
        by month, with a maximum of 10,000 data points distributed across selected years.
        """
        data = dataset_data().copy()
        if data.empty:
            print("No data available for Job Waiting Time by Month")
            return go.Figure()

        # Filter by selected years
        selected_years = list(map(int, input.years()))
        data = data[data['year'].isin(selected_years)]

        # Limit the number of points per year
        max_points = 10000
        points_per_year = max_points // len(selected_years)

        # Downsample each year
        downsampled_data = []
        for year in selected_years:
            year_data = data[data['year'] == year]
            if len(year_data) > points_per_year:
                year_data = year_data.sample(n=points_per_year, random_state=42)
            downsampled_data.append(year_data)

        # Combine downsampled data
        data = pd.concat(downsampled_data)

        # Convert waiting time from seconds to hours
        data['job_waiting_time (hours)'] = data['first_job_waiting_time'] / 3600
        data['cpu_group'] = data['slots'].apply(label_cpu_group)

        # Ensure consistent month order
        data['month'] = pd.Categorical(data['month'], categories=month_order, ordered=True)

        # Create box plot with facets by CPU group
        fig = px.box(
            data,
            x='month',
            y='job_waiting_time (hours)',
            color='year',
            facet_col='cpu_group',
            title="Job Waiting Time by Month and CPU Group (Box Plot in Hours)",
            labels={
                "job_waiting_time (hours)": "Job Waiting Time (hours)",
                "cpu_group": "CPU Group"
            }
        )

        # Adjust layout and appearance
        fig.update_layout(yaxis=dict(range=[0, 20]), boxmode='group', title=None, showlegend=True)
        for annotation in fig["layout"]["annotations"]:
            if "text" in annotation and annotation["text"].startswith("CPU Group="):
                annotation["text"] = annotation["text"].replace("CPU Group=", "")

        return fig


    # -------------------- Reactive Effects & Event Handling --------------------

    @reactive.effect
    @reactive.event(input.select_all)
    def _():
        """
        Example effect: If there's a 'select_all' checkbox or button
        for job_type, it updates the job_type selections to only OMP jobs.
        (Currently not used, but kept for reference.)
        """
        omp_jobs = [job for job in dataset.job_type.unique() if "OMP" in job]
        ui.update_checkbox_group("job_type", selected=omp_jobs)

    @reactive.effect
    @reactive.event(input.unselect_all)
    def _():
        """
        Example effect: Unselect all job_type checkboxes if needed.
        (Currently not used, but kept for reference.)
        """
        ui.update_checkbox_group("job_type", selected=[])

    @reactive.effect
    @reactive.event(input.cpus)
    def _():
        """
        This effect triggers whenever 'cpus' selection changes.
        You can insert additional logic if you need to react
        to CPU selection changes (e.g., additional UI updates).
        """
        selected_cpus = input.cpus()
        filtered_jobs = dataset[dataset['slots'].isin(get_expanded_cpu_selection(selected_cpus))]
        # Do something with filtered_jobs if needed

    @reactive.effect
    @reactive.event(input.select_all_cpus)
    def _():
        """
        Select all CPU group labels and set the month selection to all months.
        """
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

    @reactive.effect
    @reactive.event(input.unselect_all_cpus)
    def _():
        """
        Unselect all CPU group labels and months.
        """
        ui.update_checkbox_group("cpus", selected=[])
        ui.update_checkbox_group("months", selected=[])
