import pandas as pd
import numpy as np
import datetime

def determine_job_type(df): 
    def get_job_type(row):
        if row['options'] and 'gpus=' in row['options']:
            return 'GPU'
        elif row['slots'] == 1:
            return '1-p'
        elif row['granted_pe'] is not None and any(keyword in row['granted_pe'] for keyword in ['tasks_per_node', 'mpi_', 'mpi128']):
            return 'MPI'
        else:
            return 'OMP'

    df['job_type'] = df.apply(get_job_type, axis=1)
    return df

    
def check_shared_buyin(df):
    # Load the queue information from the CSV file
    queue_info_path = '/projectnb/scv/utilization/katia/queue_info.csv'
    queue_info = pd.read_csv(queue_info_path)

    class_user_dict = dict(zip(queue_info['queuename'], queue_info['class_user']))
    class_own_dict = dict(zip(queue_info['queuename'], queue_info['class_own']))

    # Map class_user and class_own to the main DataFrame based on 'qname'
    df['class_user'] = df['qname'].map(class_user_dict)
    df['class_own'] = df['qname'].map(class_own_dict)

    return df
