import pandas as pd

# Path to the log file
log_file_path = '../data/conn4.log.labeled'

# Read the log file
with open(log_file_path, 'r') as file:
    lines = file.readlines()

# Extract headers (typically the 8th line)
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

# Convert 'ts' field to datetime if present
if 'ts' in df.columns:
    df['ts'] = pd.to_datetime(df['ts'], unit='s')

# Save to CSV
df.to_csv('conn4_log_labeled.csv', index=False)

print("Conversion complete. CSV saved as 'conn_log_labeled.csv'")
