import pandas as pd
from datetime import datetime, timedelta

# Create a sample dataset
data = []
start_time = datetime(2025, 4, 27, 10, 0, 0)

regions = ['RegionA', 'RegionB']
metrics = ['PM2.5', 'temp', 'humidity']

for i in range(5):  # 5 timestamps, you can increase
    for region in regions:
        timestamp = start_time + timedelta(minutes=5 * i)
        for metric in metrics:
            if metric == 'PM2.5':
                value = 50 + i * 2 + (0 if region == 'RegionA' else 10)
            elif metric == 'temp':
                value = 22 + i + (0 if region == 'RegionA' else 1)
            else:  # humidity
                value = 40 + i * 2 + (5 if region == 'RegionB' else 0)
            data.append([timestamp.strftime("%Y-%m-%d %H:%M:%S"), region, metric, value])

# Create DataFrame
df = pd.DataFrame(data, columns=["timestamp", "region", "metric", "value"])

# Save to CSV
df.to_csv("pollution_data.csv", index=False)

print("pollution_data.csv generated successfully!")
