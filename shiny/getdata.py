import os
import pandas as pd
import pyarrow.feather as feather
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
import argparse

def process_chunk(chunk, accounting_cols_types):
    chunk = chunk.astype({col: col_type for col, col_type in accounting_cols_types.items() if col in chunk.columns}, errors='ignore')
    chunk['hostname'] = chunk['hostname'].astype(str)
    chunk = chunk[chunk['ux_start_time'] > 0]
    chunk['hostname'] = chunk['hostname'].str.extract(r'([^.]+)')
    chunk['job_name'] = chunk['job_name'].astype(str).str.encode('utf-8', 'ignore').str.decode('utf-8')
    return chunk

# def acct_readfile(datafilepath, chunk_size=1000000):
#     accounting_cols_names = [
#         "qname",
#         "hostname",
#         "owner",
#         "job_name",
#         "job_number",
#         "ux_submission_time",
#         "ux_start_time",
#         "ux_end_time",
#         "failed",
#         "exit_status",
#         "ru_wallclock",
#         "ru_utime",
#         "ru_maxrss",
#         "project",
#         "granted_pe",
#         "slots",
#         "task_number",
#         "cpu",
#         "options",
#         "pe_taskid",
#         "maxvmem"
#     ]

#     accounting_cols_types = {
#         "job_number": 'Int64',
#         "ux_submission_time": 'Int64',
#         "ux_start_time": 'Int64',
#         "ux_end_time": 'Int64',
#         "failed": 'Int64',
#         "exit_status": 'Int64',
#         "ru_wallclock": 'Int64',
#         "ru_utime": 'float',
#         "ru_maxrss": 'float',
#         "slots": 'Int64',
#         "task_number": 'Int64',
#         "cpu": 'float',
#         "maxvmem": 'float'
#     }

#     chunks = []
#     with pd.read_csv(
#         datafilepath,
#         sep=":",
#         header=None,
#         names=accounting_cols_names,
#         na_values="NONE",
#         comment='#',
#         encoding='latin1',
#         low_memory=False,
#         chunksize=chunk_size, 
#         # on_bad_lines='skip'  # Skips problematic rows
#     ) as reader:
#         with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
#             futures = [executor.submit(process_chunk, chunk, accounting_cols_types) for chunk in reader]
#             for future in futures:
#                 chunks.append(future.result())

#     df = pd.concat(chunks, ignore_index=True)
#     return df

# def acct_rcsmetrics(output_datapath, datayear=None, dataset="scc"):
#     if datayear is None:
#         datayear = pd.to_datetime('today').year
    
#     if dataset not in ["scc", "ood"]:
#         raise ValueError("Error: accounting file dataset must be scc or ood")
    
#     datapath = "/projectnb/rcsmetrics/accounting/data"  # read source file from rcsmetrics/accounting
    
#     if dataset == "scc":
#         # datafile = os.path.join(datapath, dataset, str(datayear))
#         datafile = os.path.join(datapath, dataset, f"{datayear}.feather")
        
#         if not os.path.exists(datafile):
#             print("Prerotate Condition!")
#             print(f"File does not exist: {datafile}")
#             datafile = os.path.join(datapath, dataset, str(int(datayear) - 1))
#             print(f"Using previous year: {datafile}")
#     else:
#         datafilename = "accounting"
#         datafile = os.path.join(datapath, dataset, datafilename)
    
#     acct = acct_readfile(datafile)
    
#     # output_csv_path = os.path.join(output_datapath, f"{datayear}-test.csv")
#     output_feather_path = os.path.join(output_datapath, f"{datayear}-test.feather")

#     print("start writing output csv")
#     acct.to_csv(output_csv_path, index=False)
#     print("start writing output feather")
#     feather.write_feather(acct, output_feather_path)
#     print("task completed!")

# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description='Process accounting data.')
#     parser.add_argument('year', type=int, help='Year of the data to process')
#     args = parser.parse_args()
    
#     output_datapath = "/projectnb/rcs-intern/Jiazheng/accounting/data/scc"

#     acct_rcsmetrics(output_datapath=output_datapath, datayear=args.year, dataset="scc")

def acct_readfile(datafilepath):
    accounting_cols_types = {
        "job_number": 'Int64',
        "ux_submission_time": 'Int64',
        "ux_start_time": 'Int64',
        "ux_end_time": 'Int64',
        "failed": 'Int64',
        "exit_status": 'Int64',
        "ru_wallclock": 'Int64',
        "ru_utime": 'float',
        "ru_maxrss": 'float',
        "slots": 'Int64',
        "task_number": 'Int64',
        "cpu": 'float',
        "maxvmem": 'float'
    }

    df = feather.read_feather(datafilepath)
    
    with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
        future = executor.submit(process_chunk, df, accounting_cols_types)
        processed_df = future.result()

    return processed_df

def acct_rcsmetrics(output_datapath, datayear=None, dataset="scc"):
    if datayear is None:
        datayear = pd.to_datetime('today').year
    
    if dataset not in ["scc", "ood"]:
        raise ValueError("Error: accounting file dataset must be scc or ood")
    
    datapath = "/projectnb/rcsmetrics/accounting/data"
    
    if dataset == "scc":
        datafile = os.path.join(datapath, dataset, f"{datayear}.feather")
        
        if not os.path.exists(datafile):
            print("Prerotate Condition!")
            print(f"File does not exist: {datafile}")
            datafile = os.path.join(datapath, dataset, f"{int(datayear) - 1}.feather")
            print(f"Using previous year: {datafile}")
    else:
        datafile = os.path.join(datapath, dataset, "accounting.feather")
    
    acct = acct_readfile(datafile)
    
    output_feather_path = os.path.join(output_datapath, f"{datayear}-test.feather")

    print("start writing output feather")
    feather.write_feather(acct, output_feather_path)
    print("task completed!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process accounting data.')
    parser.add_argument('year', type=int, help='Year of the data to process')
    args = parser.parse_args()
    
    output_datapath = "/projectnb/rcs-intern/Jiazheng/accounting/data/scc"

    acct_rcsmetrics(output_datapath=output_datapath, datayear=args.year, dataset="scc")


















# acct_rcsmetrics(dataset="ood")

# For loop to process multiple years
# for yr in range(2013, 2023):
#     start = pd.Timestamp.now()
#     acct_rcsmetrics(datayear=yr, dataset="scc")
#     end = pd.Timestamp.now()
#     print(f"{yr}: {end - start}")

# REFERENCE
# List of all accounting file fields:

# "qname", "hostname", "group", "owner", "job_name", "job_number", "account", 
# "priority", "ux_submission_time", "ux_start_time", "ux_end_time", "failed", 
# "exit_status", "ru_wallclock", "ru_utime", "ru_stime", "ru_maxrss", "ru_ixrss", 
# "ru_ismrss", "ru_idrss", "ru_isrss", "ru_minflt", "ru_majflt", "ru_nswap", 
# "ru_inblock", "ru_oublock", "ru_msgsnd", "ru_msgrcv", "ru_nsignals", "ru_nvcsw", 
# "ru_nivcsw", "project", "department", "granted_pe", "slots", "task_number", 
# "cpu", "mem", "io", "category", "iow", "pe_taskid", "maxvmem", "arid", 
# "ar_submission_time"
