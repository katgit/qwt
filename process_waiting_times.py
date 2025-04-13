import pandas as pd
from datetime import datetime
import os  
import time  

# Start the timer
start_time = time.time()

current_year = datetime.now().year

dataframes = []
# Automatically process all year CSV files into feather format
for year in range(2013, current_year + 1):
    file_path = f"/projectnb/rcs-intern/Jiazheng/accounting/waiting_times_{year}_per_job_type.csv"
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        dataframes.append(df)
    else:
        print(f"File not found: {file_path}")

dataset = pd.concat(dataframes, ignore_index=True)

# Remove 'buyin' rows
# dataset = dataset[dataset["queue_type"] != "buyin"].reset_index(drop=True)

# Basic cleaning/conversions here so we don’t do them repeatedly:
dataset["year"] = dataset["year"].astype(int, errors="ignore")

month_order = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", 
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
if "month" in dataset.columns:
    dataset["month"] = pd.Categorical(dataset["month"], categories=month_order, ordered=True)

# Drop rows with NA if needed
dataset = dataset[dataset["first_job_waiting_time"] >= 0] # drop negative value in case!
dataset.dropna(subset=["year", "job_type", "first_job_waiting_time"], inplace=True)

# Define filter functions
def filter_data_by_job_type(job_type_pattern, years=None):
    """Generic function to filter dataset by job type and years."""
    df = dataset[dataset["job_type"].str.startswith(job_type_pattern, na=False)].copy()
    if years:
        df = df[df["year"].isin(years)]
    return df

# Save each filtered dataset as a Feather and CSV file
def save_filtered_data():
    years = list(range(2013, current_year + 1))  

    # GPU jobs
    gpu_df = filter_data_by_job_type("GPU", years)
    gpu_df.reset_index(drop=True).to_feather("/projectnb/rcs-intern/Jiazheng/accounting/ShinyApp_Data_GPU.feather")
    gpu_df.to_csv("/projectnb/rcs-intern/Jiazheng/accounting/ShinyApp_Data_GPU.csv", index=False)

    # MPI jobs
    mpi_df = filter_data_by_job_type("MPI", years)
    mpi_df.reset_index(drop=True).to_feather("/projectnb/rcs-intern/Jiazheng/accounting/ShinyApp_Data_MPI.feather")
    mpi_df.to_csv("/projectnb/rcs-intern/Jiazheng/accounting/ShinyApp_Data_MPI.csv", index=False)

    # OMP jobs
    omp_df = filter_data_by_job_type("OMP", years)
    omp_df.reset_index(drop=True).to_feather("/projectnb/rcs-intern/Jiazheng/accounting/ShinyApp_Data_OMP.feather")
    omp_df.to_csv("/projectnb/rcs-intern/Jiazheng/accounting/ShinyApp_Data_OMP.csv", index=False)

    # 1-p jobs
    onep_df = filter_data_by_job_type("1-p", years)
    onep_df.reset_index(drop=True).to_feather("/projectnb/rcs-intern/Jiazheng/accounting/ShinyApp_Data_OneP.feather")
    onep_df.to_csv("/projectnb/rcs-intern/Jiazheng/accounting/ShinyApp_Data_OneP.csv", index=False)

    # Save the fully cleaned dataset
    dataset.reset_index(drop=True).to_feather("/projectnb/rcs-intern/Jiazheng/accounting/ShinyApp_Data.feather")
    dataset.to_csv("/projectnb/rcs-intern/Jiazheng/accounting/ShinyApp_Data.csv", index=False)

# Usage
if __name__ == "__main__":
    save_filtered_data()
    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    print(f"All files have been successfully output in {elapsed_time:.2f} seconds.")