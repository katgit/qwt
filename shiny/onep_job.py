import faicons as fa
import pandas as pd
from shiny import ui, render, reactive
from shinywidgets import output_widget, render_plotly
import plotly.express as px
import plotly.graph_objects as go

# ---------------------------------------
# DATA LOADING
# ---------------------------------------
dataset = pd.read_feather("/projectnb/rcs-intern/Jiazheng/accounting/ShinyApp_Data_OneP.feather")

# Optional: remove rows with NaN values if needed
# dataset.dropna(inplace=True)

# Ensure 'year' is integer (if needed, uncomment)
# dataset['year'] = dataset['year'].astype(int)

# ---------------------------------------
# ICONS
# ---------------------------------------
ICONS = {
    "min": fa.icon_svg("arrow-down"),
    "max": fa.icon_svg("arrow-up"),
    "mean": fa.icon_svg("users"),  # or "speed" if you prefer
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


# ---------------------------------------
# UI DEFINITION
# ---------------------------------------
def oneP_job_ui():
    """
    UI for the 1-p Job page. 
    No month selection here, only year selection and some summary boxes + a boxplot.
    """
    return ui.page_fluid(
        ui.input_checkbox_group(
            "years",
            "Select Year(s)",
            list(range(2013, 2026)),  # 2013–2025
            selected=[2024],
            inline=True
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
                    "Box Plot of Job Waiting Time by Year",
                    class_="d-flex justify-content-between align-items-center"
                ),
                output_widget("job_waiting_time_by_year"),
                full_screen=True
            ),
            col_widths=[12]
        ),
        fillable=True,
    )


# ---------------------------------------
# SERVER LOGIC
# ---------------------------------------
def oneP_job_server(input, output, session):
    """
    Server logic for the 1-p Job page.
    - Filters dataset to "1-p" job-type (if needed) and selected years.
    - Calculates summary stats (min, max, mean, median, count).
    - Renders a data table and box plot.
    """

    @reactive.Calc
    def oneP_filtered_data():
        """
        Filter the dataset to the selected years (and potentially '1-p' job_type if necessary).
        Returns the subset used for all subsequent stats and plotting.
        """
        years = list(map(int, input.years()))
        if not years:
            # If nothing is selected, return empty
            return dataset.iloc[0:0]

        # Filter by years
        filtered_df = dataset[dataset["year"].isin(years)]

        # If your dataset has other job types, filter to '1-p' here:
        # filtered_df = filtered_df[filtered_df["job_type"].str.contains("1-p", na=False)]

        return filtered_df

    @reactive.Calc
    def oneP_waiting_time_stats():
        """
        Calculate min, max, mean, median, and count (in minutes) 
        for the filtered dataset's 'first_job_waiting_time'.
        """
        df = oneP_filtered_data()
        if df.empty:
            return dict(min=None, max=None, mean=None, median=None, count=0)

        # Convert seconds -> minutes
        waiting_times_min = df["first_job_waiting_time"] / 60.0
        return dict(
            min=max(waiting_times_min.min(), 0),
            max=waiting_times_min.max(),
            mean=waiting_times_min.mean(),
            median=waiting_times_min.median(),
            count=df.shape[0],
        )

    # ----------------------------------------------------------------
    # VALUE BOX RENDERERS
    # ----------------------------------------------------------------
    @output
    @render.text
    def min_waiting_time():
        stats = oneP_waiting_time_stats()
        if stats["min"] is None:
            return "No data available"
        # If min waiting time is more than 60 minutes, show in hours
        return f"{stats['min'] / 60:.1f} hours" if stats["min"] > 60 else f"{stats['min']:.1f} min"

    @output
    @render.text
    def max_waiting_time():
        stats = oneP_waiting_time_stats()
        if stats["max"] is None:
            return "No data available"
        return f"{stats['max'] / 60:.1f} hours" if stats["max"] > 60 else f"{stats['max']:.1f} min"

    @output
    @render.text
    def mean_waiting_time():
        stats = oneP_waiting_time_stats()
        if stats["mean"] is None:
            return "No data available"
        return f"{stats['mean'] / 60:.1f} hours" if stats["mean"] > 60 else f"{stats['mean']:.1f} min"

    @output
    @render.text
    def median_waiting_time():
        stats = oneP_waiting_time_stats()
        if stats["median"] is None:
            return "No data available"
        return f"{stats['median'] / 60:.1f} hours" if stats["median"] > 60 else f"{stats['median']:.1f} min"

    @output
    @render.text
    def job_count():
        stats = oneP_waiting_time_stats()
        return str(stats["count"])

    # ----------------------------------------------------------------
    # TABLE RENDERER
    # ----------------------------------------------------------------
    # @output
    # @render.data_frame
    # def displayTable():
    #     """
    #     Show the filtered data in a table, with waiting time in minutes.
    #     """
    #     df = oneP_filtered_data()
    #     if df.empty:
    #         return pd.DataFrame()

    #     # Copy once for display manipulation
    #     df_disp = df.copy()
    #     # Convert waiting time from seconds -> minutes for display
    #     df_disp["first_job_waiting_time"] = (df_disp["first_job_waiting_time"] / 60).round(1)
    #     df_disp.sort_values(by="job_number", inplace=True)

    #     # Rename columns just for display
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

    # ----------------------------------------------------------------
    # BOX PLOT
    # ----------------------------------------------------------------
    @render_plotly
    def job_waiting_time_by_year():
        """
        Box plot of waiting time vs. year. 
        Show only the top 20 job types with the most points.
        """
        df = oneP_filtered_data()
        if df.empty:
            return go.Figure()

        df_plot = df.copy()
        df_plot["job_waiting_time (hours)"] = df_plot["first_job_waiting_time"] / 3600.0

        # Find the top 20 job types with the most points
        top_job_types = (
            df_plot["job_type"]
            .value_counts()
            .head(20)  # Get the top 20 job types
            .index.tolist()
        )

        # Filter the dataset to include only the top 20 job types
        df_plot = df_plot[df_plot["job_type"].isin(top_job_types)]

        # Create the box plot
        fig = px.box(
            df_plot,
            x="year",
            y="job_waiting_time (hours)",
            color="year",
            facet_col="job_type",  # Facet by the top 20 job types
            facet_col_spacing=0.01,  # Adjust horizontal spacing for 20 columns
            labels={"job_waiting_time (hours)": "Job Waiting Time (hours)"},
        )
        fig.update_layout(
            yaxis=dict(range=[0, 20]),  # Example: focus on up to 20 hours
            boxmode="group",
            showlegend=True,
            title=None
        )
        # Remove "job_type=" prefix in facet titles
        for annotation in fig["layout"]["annotations"]:
            if annotation["text"].startswith("job_type="):
                annotation["text"] = annotation["text"][12:]
        # Remove x-axis label for each subplot
        for axis in fig.layout:
            if axis.startswith("xaxis"):
                fig.layout[axis].title.text = None

        return fig



    # ----------------------------------------------------------------
    # OPTIONAL: "Select All" / "Unselect All" placeholders
    # ----------------------------------------------------------------
    @reactive.effect
    @reactive.event(input.select_all)
    def _():
        pass

    @reactive.effect
    @reactive.event(input.unselect_all)
    def _():
        pass
