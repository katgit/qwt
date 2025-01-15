from pathlib import Path
import pandas as pd

app_dir = Path(__file__).parent

dataframes = []
for year in range(2013, 2025):
    file_path = app_dir / f"../../waiting_times_{year}_per_job_type.csv"
    if file_path.exists():
        df = pd.read_csv(file_path)
        dataframes.append(df)

dataset = pd.concat(dataframes, ignore_index=True)

# Remove 'buyin' rows
dataset = dataset[dataset["queue_type"] != "buyin"].reset_index(drop=True)

# Basic cleaning/conversions here so we don’t do them repeatedly:
dataset["year"] = dataset["year"].astype(int, errors="ignore")

# If your month column is always present:
month_order = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", 
                "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
if "month" in dataset.columns:
    dataset["month"] = pd.Categorical(dataset["month"], categories=month_order, ordered=True)

# Drop rows with NA if needed
dataset.dropna(subset=["year", "job_type", "first_job_waiting_time"], inplace=True)

# Create a copy of the dataset before renaming job_type
cleaned_dataset = dataset.copy()

def standardize_job_type(job_type):
    """Standardize job types into GPU, MPI, OMP, or 1-p."""
    if "GPU" in job_type:
        return "GPU"
    elif "MPI" in job_type:
        return "MPI"
    elif "omp" in job_type.lower():  # Case-insensitive match
        return "OMP"
    elif "1-p" in job_type:
        return "1-p"
    else:
        return None  # For non-matching job types

# Apply the function to standardize job types
cleaned_dataset["job_type"] = cleaned_dataset["job_type"].apply(standardize_job_type)

# Filter out rows where job_type is None (non-matching rows)
cleaned_dataset = cleaned_dataset[cleaned_dataset["job_type"].notnull()]



# Define filter functions
def filter_data_by_job_type(job_type_pattern, years=None):
    """Generic function to filter dataset by job type and years."""
    df = dataset[dataset["job_type"].str.startswith(job_type_pattern, na=False)].copy()
    if years:
        df = df[df["year"].isin(years)]
    return df

# Save each filtered dataset as a Feather file
def save_filtered_data():
    years = list(range(2013, 2025))  # Adjust years as needed

    # GPU jobs
    gpu_df = filter_data_by_job_type("GPU", years)
    gpu_df.reset_index(drop=True).to_feather(app_dir / "../../ShinyApp_Data_GPU.feather")
    gpu_df.reset_index(drop=True).to_csv(app_dir / "../../ShinyApp_Data_GPU.csv", index=False)

    # MPI jobs
    mpi_df = filter_data_by_job_type("MPI", years)
    mpi_df.reset_index(drop=True).to_feather(app_dir / "../../ShinyApp_Data_MPI.feather")
    mpi_df.reset_index(drop=True).to_csv(app_dir / "../../ShinyApp_Data_MPI.csv", index=False)

    # OMP jobs
    omp_df = filter_data_by_job_type("omp", years)
    omp_df.reset_index(drop=True).to_feather(app_dir / "../../ShinyApp_Data_OMP.feather")
    omp_df.reset_index(drop=True).to_csv(app_dir / "../../ShinyApp_Data_OMP.csv", index=False)

    # 1-p jobs
    onep_df = filter_data_by_job_type("1-p", years)
    onep_df.reset_index(drop=True).to_feather(app_dir / "../../ShinyApp_Data_OneP.feather")
    onep_df.reset_index(drop=True).to_csv(app_dir / "../../ShinyApp_Data_OneP.csv", index=False)

    # Save the fully cleaned dataset
    cleaned_dataset.reset_index(drop=True).to_feather(app_dir / "../../ShinyApp_Data.feather")
    cleaned_dataset.reset_index(drop=True).to_csv(app_dir / "../../ShinyApp_Data.csv", index=False)



def compare_files(app_dir):
    # Define the file pairs to compare
    file_pairs = [
        ("../../ShinyApp_Data_GPU.feather", "../../ShinyApp_Data_GPU.csv"),
        ("../../ShinyApp_Data_MPI.feather", "../../ShinyApp_Data_MPI.csv"),
        ("../../ShinyApp_Data_OMP.feather", "../../ShinyApp_Data_OMP.csv"),
        ("../../ShinyApp_Data_OneP.feather", "../../ShinyApp_Data_OneP.csv"),
        ("../../ShinyApp_Data.feather", "../../ShinyApp_Data.csv")
    ]
    
    for feather_file, csv_file in file_pairs:
        # Resolve full file paths
        feather_path = app_dir / feather_file
        csv_path = app_dir / csv_file
        
        # Load the Feather and CSV files
        feather_df = pd.read_feather(feather_path)
        csv_df = pd.read_csv(csv_path)
        
        # Compare row counts
        feather_rows = len(feather_df)
        csv_rows = len(csv_df)
        print(f"Comparing {feather_file} and {csv_file}:")
        print(f"  Feather rows: {feather_rows}")
        print(f"  CSV rows: {csv_rows}")
        
        # Check if the rows are equal
        if feather_rows == csv_rows:
            print("  Row counts match!")
        else:
            print("  Row counts do NOT match!")
        
        # Optionally, compare column names and contents
        feather_columns = set(feather_df.columns)
        csv_columns = set(csv_df.columns)
        if feather_columns != csv_columns:
            print("  Column names do NOT match!")
        else:
            print("  Column names match.")

        print("-" * 50)

# Usage
if __name__ == "__main__":
    app_dir = Path(__file__).parent
    # compare_files(app_dir)
