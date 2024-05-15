import pandas as pd
import glob
import os

def process_conn_log(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()

    # Extract headers (assuming headers are always at the same line, e.g., line 8)
    headers = lines[7].strip().split()
    num_columns = len(headers)

    # Extract data (lines after headers) and ensure each row has the correct number of columns
    data = []
    for line in lines[8:]:
        row = line.strip().split()
        if len(row) < num_columns:
            row.extend([None] * (num_columns - len(row)))  # Pad missing values
        elif len(row) > num_columns:
            row = row[:num_columns]  # Truncate extra values
        data.append(row)

    # Create a DataFrame
    df = pd.DataFrame(data, columns=headers)

    # Convert types if necessary
    if 'ts' in df.columns:
        df['ts'] = pd.to_datetime(df['ts'], unit='s')

    return df

def combine_conn_logs(log_dir):
    all_files = glob.glob(os.path.join(log_dir, '*'))
    df_list = [process_conn_log(file) for file in all_files]
    combined_df = pd.concat(df_list, ignore_index=True)
    return combined_df

def save_combined_csv(df, output_file):
    df.to_csv(output_file, index=False)
    print(f"Combined CSV saved as '{output_file}'")

if __name__ == "__main__":
    log_directory = 'iot_23_datasets_small/opt/Malware-Project/BigDataset/IoTScenarios/all_dataset'
    output_csv = 'combined_conn_log.csv'

    combined_df = combine_conn_logs(log_directory)
    save_combined_csv(combined_df, output_csv)
