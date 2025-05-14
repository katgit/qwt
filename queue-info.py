#!/usr/bin/env python3
import sys
import pandas as pd
from pathlib import Path

# Helper function to format time
def format_time(seconds):
    if seconds >= 3600:  
        return f"{seconds / 3600:.2f} hour"
    elif seconds >= 60:  
        return f"{seconds / 60:.2f} min"
    else:  
        return f"{seconds:.2f} sec"

# Check command-line arguments
if len(sys.argv) != 3:
    print("Usage: queue-info.py <year> <month>")
    sys.exit(1)

year = int(sys.argv[1])
month = int(sys.argv[2])

# Define the path to the Feather files
base_path = Path("/projectnb/rcs-intern/Jiazheng/accounting")
job_types = ["GPU", "MPI", "OMP", "OneP"]

# Map month number to month abbreviation (e.g., 4 -> "Apr")
month_abbr = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", 
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
month_name = month_abbr[month - 1]  # Convert month number to name

# Print table header
print()
print(f"Queue Waiting Time Basic Info of {year} {month_abbr[month - 1]}")
print()
print(f"{'Job Type':<10} {'Min':<15} {'Max':<15} {'Mean':<15} {'Median':<15} {'First Waiting Jobs':<10}")
print("-" * 95)

# Process each job type
for job_type in job_types:
    file_path = base_path / f"ShinyApp_Data_{job_type}.feather"
    
    # Read the Feather file
    try:
        df = pd.read_feather(file_path)
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        continue

    # Filter rows by year and month
    filtered_df = df[(df["year"] == year) & (df["month"] == month_name)]

    # Calculate statistics
    if not filtered_df.empty:
        waiting_times = filtered_df["first_job_waiting_time"]
        min_val = format_time(waiting_times.min())
        max_val = format_time(waiting_times.max())
        mean_val = format_time(waiting_times.mean())
        median_val = format_time(waiting_times.median())
        total_jobs = len(waiting_times)

        # Print results in a horizontal table format
        print(f"{job_type:<10} {min_val:<15} {max_val:<15} {mean_val:<15} {median_val:<15} {total_jobs:<10}")
    else:
        print(f"{job_type:<10} {'No data found':<70}")