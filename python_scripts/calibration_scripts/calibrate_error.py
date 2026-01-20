import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Read and filter data
df = pd.read_csv('/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/output/NET2_jobs_output_jan_1_2_4.csv')
df = df[df['STATUS'] == "finished"]
df = df[df['CORES'] == 1]

# Compute errors
df['error'] = df['CPU_CONSUMPTION_TIME'] - df['EXECUTION_TIME']
df['absolute_error'] = df['error'].abs()
df['relative_error'] = (df['absolute_error'] / df['EXECUTION_TIME']) * 100

# Compute summary statistics per SITE
error_stats = df.groupby('SITE').agg({
    'error': ['mean', 'std'],
    'absolute_error': ['mean', 'std'],
    'relative_error': ['mean', 'std']
})
print("Error Statistics by SITE:")
print(error_stats)

# Plot mean absolute error per SITE
mean_abs_error = df.groupby('SITE')['absolute_error'].mean()
plt.figure(figsize=(10, 6))
mean_abs_error.plot(kind='bar')
plt.xlabel('SITE')
plt.ylabel('Mean Absolute Error (Seconds)')
plt.title('Mean Absolute Error by SITE')
plt.tight_layout()
plt.show()

# Plot overall error distribution
plt.figure(figsize=(10, 6))
df['absolute_error'].hist(bins=30)
plt.xlabel('Absolute Error (Seconds)')
plt.ylabel('Frequency')
plt.title('Distribution of Absolute Errors')
plt.tight_layout()
plt.show()
