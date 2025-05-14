import pandas as pd
import numpy as np
import time
import argparse
from tqdm import tqdm
from helpers import (determine_job_type, check_shared_buyin)
import datetime


def waiting_time_per_job_type(input_file_name, output_file_name, year):
    # Read data from the Feather file
    df = pd.read_feather(input_file_name)

    # filter cols we need
    df = df[['ux_submission_time', 'ux_start_time', 'ux_end_time', 'granted_pe', 'slots', 'options', 'pe_taskid', 'qname', 'job_number', 'owner', 'job_name', 'task_number']]
    # remove records: 'mpi' exist in granted_pe and its pe_taskid column has a valid value
    df = df[~((df['granted_pe'].str.contains('mpi', na=False)) & (df['pe_taskid'].notna() & df['pe_taskid'].ne('None')))]
    
    # df['job_type'] = df.apply(determine_job_type, axis=1)
    print('Total jobs:', len(df))
    df = determine_job_type(df)
    print('after determined job type:', len(df))
    print(pd.crosstab(index=df.job_type, columns="count"))
    df = check_shared_buyin(df)
    df.sort_values(by='ux_end_time', inplace=True)
    print('after sorting by ending_time:', pd.crosstab(index=df.job_type, columns="count"))

    # Ensure the time columns are integers
    df['ux_submission_time'] = df['ux_submission_time'].astype(int)
    df['ux_start_time'] = df['ux_start_time'].astype(int)
    df['ux_end_time'] = df['ux_end_time'].astype(int)
    df['year'] = year

    
    latest_end_times = {} # Initialize a dictionary to track the latest end time for each (owner, job_type)
    job_type_waiting_times = [] # Initialize a list to store waiting times

    for index, row in tqdm(df.iterrows(), total=len(df), desc="Processing jobs"):
        submission_time = row['ux_submission_time']
        start_time = row['ux_start_time']
        end_time = row['ux_end_time']
        owner = row['owner']
        job_type = row['job_type']
        qname = row['qname']
        initialYear = datetime.datetime.fromtimestamp(submission_time).year

        # Only process jobs from the specified year
        if initialYear == year:
            month = datetime.datetime.fromtimestamp(submission_time).strftime('%b')
            day = datetime.datetime.fromtimestamp(submission_time).day  # Extract 1–31

            # Initialize the latest end time for this (owner, job_type) if not already set
            if (owner, job_type) not in latest_end_times:
                latest_end_times[(owner, job_type)] = 0

            # Check if this job is a "first job" (submission_time > latest_end_time)
            if submission_time > latest_end_times[(owner, job_type)]:
                current_waiting_time = start_time - submission_time
                if current_waiting_time >= 0:
                    # Determine job_type_label based on job_type and qname
                    if job_type == 'GPU':
                        if 'gpus=1' in row['options']:
                            job_type_label = f'GPU = 1 {qname}'
                        else:
                            job_type_label = f'GPU > 1 {qname}'
                    elif job_type == 'MPI':
                        job_type_label = f'MPI job {qname}'
                    else:
                        job_type_label = f'{job_type} {qname}'

                    # Append the result
                    job_type_waiting_times.append([
                        job_type_label,
                        row['class_user'],
                        row['class_own'],
                        current_waiting_time,
                        month,
                        year,
                        day,
                        row['job_number'],
                        row['slots']
                    ])

            # Update the latest end time for this (owner, job_type)
            latest_end_times[(owner, job_type)] = max(end_time, latest_end_times[(owner, job_type)])

    # Convert the results to a DataFrame
    job_type_waiting_df = pd.DataFrame(job_type_waiting_times, columns=['job_type', 'class_user', 'class_own', 'first_job_waiting_time', 'month', 'year', 'day', 'job_number', 'slots'])

    # Save the results to a CSV file
    job_type_waiting_df.to_csv(output_file_name, index=False, chunksize=100000)
    



# Example usage
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process accounting data.')
    parser.add_argument('year', type=int, help='Year of the data to process')
    args = parser.parse_args()

    year = args.year
    input_file_name = f'/projectnb/rcsmetrics/accounting/data/scc/{year}.feather'
    output_file_name = f'/projectnb/rcs-intern/Jiazheng/accounting/waiting_times_{year}_per_job_type.csv'

    start_time = time.time()
    waiting_time_per_job_type(input_file_name, output_file_name, year)
    end_time = time.time()

    running_time = end_time - start_time

    print(f"First job waiting times per job type saved to {output_file_name}")
    print(f"Running time: {running_time} seconds")

