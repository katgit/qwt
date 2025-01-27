import pandas as pd
import numpy as np
import time
import argparse
from tqdm import tqdm
from helpers import (determine_job_type, determine_job_type, check_shared_buyin, GPU_1_queue_time, GPU_1_queue_time_by_month, GPU_all_queue_time, GPU_queue_time_by_month, MPI_shared_queues_time, MPI_shared_queue_separately, calculate_statistics)
import datetime


def waiting_time_per_job_type(input_file_name, output_file_name, year):
    # Read data from the Feather file
    df = pd.read_feather(input_file_name)

    # filter cols we need
    df = df[['ux_submission_time', 'ux_start_time', 'ux_end_time', 'granted_pe', 'slots', 'options', 'pe_taskid', 'qname', 'job_number', 'owner', 'job_name', 'task_number']]
    # df = df[:1000000]
    # Apply the function to create the job_type column
    # df['job_type'] = df.apply(determine_job_type, axis=1)
    print('Total jobs:', len(df))
    df = determine_job_type(df)
    print('after determined job type:', len(df))
    print(pd.crosstab(index=df.job_type, columns="count"))
    df = check_shared_buyin(df)
    df.sort_values(by='ux_submission_time', inplace=True)
    print('after sorting by submission_time:', pd.crosstab(index=df.job_type, columns="count"))

    # Ensure the time columns are integers
    df['ux_submission_time'] = df['ux_submission_time'].astype(int)
    df['ux_start_time'] = df['ux_start_time'].astype(int)
    df['ux_end_time'] = df['ux_end_time'].astype(int)

    df['year'] = year

    job_type_waiting_times = []
    
    for owner, owner_group in tqdm(df.groupby('owner'), desc="Processing owners"):
        for job_type, group in owner_group.groupby('job_type'):

            if job_type == 'GPU':
                count = 0
                months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                latest_end_times = {month: 0 for month in months}
                gpu_1_latest_end_times = {month: 0 for month in months}

                for index, row in group.iterrows():
                    submission_time = row['ux_submission_time']
                    start_time = row['ux_start_time']
                    end_time = row['ux_end_time']
                    initialYear = datetime.datetime.fromtimestamp(submission_time).year
                    if initialYear == year:
                        month = datetime.datetime.fromtimestamp(submission_time).strftime('%b')

                        if 'gpus=1' in row['options']:
                            if submission_time > gpu_1_latest_end_times[month]:
                                current_waiting_time = start_time - submission_time
                                if current_waiting_time >= 0:
                                    job_type_waiting_times.append(
                                        ['GPU = 1', row['queue_type'], current_waiting_time, month, year, row['job_number'], row['slots']]
                                    )
                                    count += 1
                            if end_time > gpu_1_latest_end_times[month]:
                                gpu_1_latest_end_times[month] = end_time

                        else:
                            if submission_time > latest_end_times[month]:
                                current_waiting_time = start_time - submission_time
                                if current_waiting_time >= 0:
                                    job_type_waiting_times.append(
                                        ['GPU > 1', row['queue_type'], current_waiting_time, month, year, row['job_number'], row['slots']]
                                    )
                                    count += 1
                            if end_time > latest_end_times[month]:
                                latest_end_times[month] = end_time
                # print(f"Rows number GPU for owner {owner}:", count)

            elif job_type == 'MPI':
                count = 0
                qnames = list(
                    set(df['qname'].dropna().unique()).union(
                        set(df['granted_pe'].dropna().unique())
                    )
                )
                latest_end_times = {qname: 0 for qname in qnames}

                for index, row in group.iterrows():
                    submission_time = row['ux_submission_time']
                    start_time = row['ux_start_time']
                    end_time = row['ux_end_time']
                    initialYear = datetime.datetime.fromtimestamp(submission_time).year
                    if initialYear == year:
                        month = datetime.datetime.fromtimestamp(submission_time).strftime('%b')
                        qname = row['qname']
                        if qname == 'a128':
                            qname = 'a'
                        if submission_time > latest_end_times[qname]:
                            current_waiting_time = start_time - submission_time
                            if current_waiting_time >= 0:
                                job_type_waiting_times.append(
                                    [f'MPI job {qname}', row['queue_type'], current_waiting_time, month, year, row['job_number'], row['slots']]
                                )
                                count += 1

                        latest_end_times[qname] = max(end_time, latest_end_times[qname])
                # print(f"Rows number MPI for owner {owner}:", count)

            else:
                count = 0
                qnames = list(
                    set(df['qname'].dropna().unique()).union(
                        set(df['granted_pe'].dropna().unique())
                    )
                )
                latest_end_times = {qname: 0 for qname in qnames}

                for index, row in group.iterrows():
                    submission_time = row['ux_submission_time']
                    start_time = row['ux_start_time']
                    end_time = row['ux_end_time']
                    initialYear = datetime.datetime.fromtimestamp(submission_time).year
                    if initialYear == year:
                        month = datetime.datetime.fromtimestamp(submission_time).strftime('%b')
                        qname = row['qname']
                        if submission_time > latest_end_times[qname]:
                            current_waiting_time = start_time - submission_time
                            if current_waiting_time >= 0:
                                job_type_waiting_times.append(
                                    [f'{job_type} {qname}', row['queue_type'], current_waiting_time, month, year, row['job_number'], row['slots']]
                                )
                                count += 1

                        latest_end_times[qname] = max(end_time, latest_end_times[qname])
                # print(f"Rows number {job_type} for owner {owner}:", count)

    # Convert the results to a DataFrame
    job_type_waiting_df = pd.DataFrame(job_type_waiting_times, columns=['job_type', 'queue_type', 'first_job_waiting_time', 'month', 'year', 'job_number', 'slots'])

    # Save the results to a CSV file
    job_type_waiting_df.to_csv(output_file_name, index=False, chunksize=100000)



# Example usage
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process accounting data.')
    parser.add_argument('year', type=int, help='Year of the data to process')
    args = parser.parse_args()

    year = args.year
    # input_file_name = f'/projectnb/rcs-intern/Jiazheng/accounting/data/scc/{year}-filtered.feather'
    input_file_name = f'/projectnb/rcsmetrics/accounting/data/scc/{year}.feather'
    output_file_name = f'/projectnb/rcs-intern/Jiazheng/accounting/waiting_times_{year}_per_job_type.csv'

    start_time = time.time()
    waiting_time_per_job_type(input_file_name, output_file_name, year)
    end_time = time.time()

    running_time = end_time - start_time

    print(f"First job waiting times per job type saved to {output_file_name}")
    print(f"Running time: {running_time} seconds")

