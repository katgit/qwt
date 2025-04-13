import pandas as pd
import numpy as np
import datetime

def determine_job_type(df): 
    def get_job_type(row):
        if row['options'] and 'gpus=' in row['options']:
            return 'GPU'
        elif row['slots'] == 1:
            return '1-p'
        elif row['granted_pe'] is not None and any(keyword in row['granted_pe'] for keyword in ['tasks_per_node', 'mpi']):
            return 'MPI'
        else:
            return 'OMP'

    df['job_type'] = df.apply(get_job_type, axis=1)
    return df

    
def check_shared_buyin(df):
    # Load the queue information from the CSV file
    queue_info_path = '/projectnb/scv/utilization/katia/queue_info.csv'
    queue_info = pd.read_csv(queue_info_path)

    # Create a dictionary mapping 'queuename' to 'class_user'
    queue_dict = dict(zip(queue_info['queuename'], queue_info['class_user']))

    # Function to determine the queue type
    def determine_queue_type(row):
        qname = row['qname']
        # Check if 'qname' exists in the queue dictionary and determine its type
        if qname in queue_dict:
            return 'buyin' if queue_dict[qname] == 'buyin' else 'shared'
        return 'unknown'  # Default to 'unknown' if qname is not found in the dictionary

    # Apply the function to the DataFrame and create a new column
    df['queue_type'] = df.apply(determine_queue_type, axis=1)

    return df

# filter GPU jobs by months:
def GPU_queue_time_by_month(df):    # input is GPU job in one year
    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    latest_end_times = {month: 0 for month in months}
    waiting_times = {month: [] for month in months}

    for index, row in df.iterrows():
        submission_time = row['ux_submission_time']
        start_time = row['ux_start_time']
        end_time = row['ux_end_time']
        month = datetime.datetime.fromtimestamp(submission_time).strftime('%b')

        if submission_time > latest_end_times[month]:
            current_waiting_time = start_time - submission_time
            waiting_times[month].append(current_waiting_time)

        if end_time > latest_end_times[month]:
            latest_end_times[month] = end_time

    total_waiting_times = [sum(waiting_times[month]) if waiting_times[month] else 0 for month in months]
    min_waiting_times = [min(waiting_times[month]) if waiting_times[month] else 0 for month in months]
    max_waiting_times = [max(waiting_times[month]) if waiting_times[month] else 0 for month in months]
    mean_waiting_times = [sum(waiting_times[month]) / len(waiting_times[month]) if waiting_times[month] else 0 for month in months]
    median_waiting_times = [sorted(waiting_times[month])[len(waiting_times[month]) // 2] if waiting_times[month] else 0 for month in months]

    return months, total_waiting_times, min_waiting_times, max_waiting_times, mean_waiting_times, median_waiting_times

# calculate the queue waiting time for 1GPU job per month
def GPU_1_queue_time_by_month(df):
    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    latest_end_times = {month: 0 for month in months}
    waiting_times = {month: [] for month in months}

    for index, row in df.iterrows():
        if 'gpus=1' in row['options']:
            submission_time = row['ux_submission_time']
            start_time = row['ux_start_time']
            end_time = row['ux_end_time']
            month = datetime.datetime.fromtimestamp(submission_time).strftime('%b')

            if submission_time > latest_end_times[month]:
                current_waiting_time = start_time - submission_time
                waiting_times[month].append(current_waiting_time)

            if end_time > latest_end_times[month]:
                latest_end_times[month] = end_time

    total_waiting_times = [sum(waiting_times[month]) if waiting_times[month] else 0 for month in months]
    min_waiting_times = [min(waiting_times[month]) if waiting_times[month] else 0 for month in months]
    max_waiting_times = [max(waiting_times[month]) if waiting_times[month] else 0 for month in months]
    mean_waiting_times = [sum(waiting_times[month]) / len(waiting_times[month]) if waiting_times[month] else 0 for month in months]
    median_waiting_times = [sorted(waiting_times[month])[len(waiting_times[month]) // 2] if waiting_times[month] else 0 for month in months]

    return months, total_waiting_times, min_waiting_times, max_waiting_times, mean_waiting_times, median_waiting_times

# calculate the queue waiting time for GPU job use no more than 1 GPU
def GPU_1_queue_time(df):  
    waiting_time = 0
    prev_latest_end_time = 0
    waiting_times = []

    for index, row in df.iterrows():
        if 'options' in row and row['options'] and 'gpus=1' in row['options']:
            submission_time = row['ux_submission_time']
            start_time = row['ux_start_time']
            end_time = row['ux_end_time']

            if prev_latest_end_time is not None and submission_time > prev_latest_end_time:
                current_waiting_time = (start_time - submission_time)
                waiting_time += current_waiting_time
                waiting_times.append(current_waiting_time)

            if end_time > prev_latest_end_time:
                prev_latest_end_time = end_time

    if waiting_times:
        min_waiting_time = min(waiting_times)
        max_waiting_time = max(waiting_times)
        mean_waiting_time = sum(waiting_times) / len(waiting_times)
        median_waiting_time = sorted(waiting_times)[len(waiting_times) // 2]
    else:
        min_waiting_time = max_waiting_time = mean_waiting_time = median_waiting_time = 0
    
    return waiting_time, min_waiting_time, max_waiting_time, mean_waiting_time, median_waiting_time

# calculate the queue waiting time for all GPU jobs
def GPU_all_queue_time(df):
    waiting_time = 0
    prev_latest_end_time = 0
    waiting_times = []

    for index, row in df.iterrows():
        # if 'options' in row and row['options'] and 'gpus=' in row['options']:
        submission_time = row['ux_submission_time']
        start_time = row['ux_start_time']
        end_time = row['ux_end_time']

        if prev_latest_end_time is not None and submission_time > prev_latest_end_time:
            current_waiting_time = (start_time - submission_time)
            waiting_time += current_waiting_time
            waiting_times.append(current_waiting_time)

        if end_time > prev_latest_end_time:
            prev_latest_end_time = end_time

    if waiting_times:
        min_waiting_time = min(waiting_times)
        max_waiting_time = max(waiting_times)
        mean_waiting_time = sum(waiting_times) / len(waiting_times)
        median_waiting_time = sorted(waiting_times)[len(waiting_times) // 2]
    else:
        min_waiting_time = max_waiting_time = mean_waiting_time = median_waiting_time = 0
    
    return waiting_time, min_waiting_time, max_waiting_time, mean_waiting_time, median_waiting_time

#calculate the queue waiting time for MPI shared jobs
def MPI_shared_queues_time(df):
    queues = {
        'u': [],
        'z': [],
        '4': [],
        'a': [],
        'as': [],
        'budge': [],
        'total': []
    }
    latest_end_times = {
        'u': 0,
        'z': 0,
        '4': 0,
        'a': 0,
        'as': 0,
        'budge': 0,
        'total': 0
    }

    for index, row in df.iterrows():
        qname = row['qname']
        submission_time = row['ux_submission_time']
        start_time = row['ux_start_time']
        end_time = row['ux_end_time']

        
        key = qname
        if qname == 'a128':
            key = 'a'

        if submission_time > latest_end_times[key]:
            current_waiting_time = (start_time - submission_time)
            queues[key].append(current_waiting_time)
            queues['total'].append(current_waiting_time)

        if end_time > latest_end_times[key]:
            latest_end_times[key] = end_time
            latest_end_times['total'] = end_time

    results = {
        'u': calculate_statistics(queues['u']),
        'z': calculate_statistics(queues['z']),
        '4': calculate_statistics(queues['4']),
        'a': calculate_statistics(queues['a']),
        'as': calculate_statistics(queues['as']),
        'budge': calculate_statistics(queues['budge']),
        'u_z_4_a_as_budge': calculate_statistics(queues['total'])
    }

    return results

#calculate waiting time for each shared MPI queue separately. consider a and a128 as the same queue. call it as 'a'
def MPI_shared_queue_separately(df):
    waiting_times = {}
    prev_latest_end_times = {}

    # Iterate over the DataFrame rows
    for index, row in df.iterrows():
        # Get the first character of the queue name to group them
        qname_group = row['qname'][0]

        # Initialize waiting time and previous end time for new queue groups
        if qname_group not in waiting_times:
            waiting_times[qname_group] = 0
            prev_latest_end_times[qname_group] = 0

        submission_time = row['ux_submission_time']
        start_time = row['ux_start_time']
        end_time = row['ux_end_time']

        # Calculate waiting time for the current queue group
        if prev_latest_end_times[qname_group] is not None and submission_time > prev_latest_end_times[qname_group]:
            waiting_times[qname_group] += (start_time - submission_time)

        # Update the latest end time for the current queue group
        if end_time > prev_latest_end_times[qname_group]:
            prev_latest_end_times[qname_group] = end_time

    return waiting_times

def calculate_statistics(queue):
        if not queue:
            return [0, 0, 0, 0, 0]
            
        total_sum = max(sum(queue), 0)
        minimum = max(min(queue), 0)
        maximum = max(max(queue), 0)
        mean = max(sum(queue) / len(queue), 0)
        median = max(np.median(queue), 0)
        
        return [total_sum, minimum, maximum, mean, median]
