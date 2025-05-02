import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
import seaborn as sns
import os
import warnings

warnings.filterwarnings("ignore")

# Path to your final predictions file
csv_file_path = "final_predictions_synthetic.csv"
chart_output_path = "../output/section-5"

# Load data
df = pd.read_csv(csv_file_path)

# ------------------- Time-Series Line Chart -------------------
fig1 = px.line(df, x='timestamp', y=['actual_pm25', 'predicted_pm25'],
               title='Actual vs Predicted PM2.5 Over Time',
               labels={"value": "PM2.5", "timestamp": "Time"})
fig1.write_html(os.path.join(chart_output_path, "pm25_predictions_chart.html"))

# ------------------- Spike Events -------------------
df['timestamp'] = pd.to_datetime(df['timestamp'])
spikes = df[df['actual_pm25'] > 35]
fig2 = px.scatter(spikes, x='timestamp', y='actual_pm25', color='region',
                  title='Spike Events > 35 µg/m³')
fig2.write_html(os.path.join(chart_output_path, "spike_events_chart.html"))

# ------------------- AQI Classification -------------------
def classify_aqi(pm):
    if pm <= 12: return "Good"
    elif pm <= 35: return "Moderate"
    else: return "Unhealthy"

df['AQI_Category'] = df['actual_pm25'].apply(classify_aqi)
fig3 = px.pie(df, names='AQI_Category', title='AQI Classification Breakdown')
fig3.write_html(os.path.join(chart_output_path, "aqi_breakdown_chart.html"))

# ------------------- Correlation Plot -------------------
if 'temperature' in df.columns and 'humidity' in df.columns:
    corr = df[['actual_pm25', 'temperature', 'humidity']].corr()
    plt.figure(figsize=(6, 4))
    sns.heatmap(corr, annot=True, cmap='coolwarm')
    plt.title("Correlation Matrix")
    plt.tight_layout()
    plt.savefig(os.path.join(chart_output_path, "correlation_heatmap.png"))
else:
    print("⚠️ Skipping correlation plot: 'temperature' or 'humidity' column missing.")
