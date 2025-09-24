import json
import subprocess
import time
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# Same sites list as in the previous script
sites = ["BNL",
   "CERN-T0",
    "MWT2",
    "NET2_Amherst",
    "IN2P3-CC",
    "RAL",
    "CERN", 
    "FZK-LCG2",
    "Vega",
    "ANALY_AGLT2_VP",
   "INFN-CNAF",
   "TOKYO",
   "SWT2_CPB",
   "AGLT2",
   "ANALY_ARNES_DIRECT",
   "IN2P3-LAPP",
   "LRZ-LMU",
   "INFN-NAPOLI-ATLAS",
   "UKI-NORTHGRID-LANCS-HEP-CEPH",
   "DESY-ZN",
   "wuppertal",
   "pic",
   "NSC",
   "DESY-HH",
   "praguelcg2",
   "UNI-FREIBURG",
   "UKI-SCOTGRID-GLASGOW_CEPH",
   "ARNES",
   "NIKHEF",
   "GoeGrid",
   "GoeGrid_LODISK",
   "ANALY_SiGNET_DIRECT",
   "GRIF-LAL",
   "CA-SFU-T2",
   "GRIF-LPNHE",
   "IN2P3-CPPM",
   "CSCS-LCG2-ALPS",
   "HPC2N",
   "BNL_OPP",
   "SARA-MATRIX",
   "IFIC",
   "INFN-FRASCATI",
   "UKI-NORTHGRID-MAN-HEP",
   "ANALY_LRZ_VP",
   "MPPMU",
   "IN2P3-LPC",
   "SiGNET-NSC",
   "WEIZMANN",
   "IL-TAU",
   "UNIBE-LHEP",
   "UNIGE-BAOBAB",
   "SiGNET",
   "BEIJING",
   "RO-14-ITIM",
   "TECHNION",
   "LRZ-LMU_TEST",
   "ifae",
   "GRIF-IRFU",
   "UAM"]

# Configuration parameters
JOBS_PER_SITE = 200  # Updated from 200 to 10000

# Base paths
base_path = "/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/build"
base_config_path = "/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/config-files/config_base.json"
config_dir = f"{base_path}/config-files/scalability_configs"
results_dir = f"{base_path}/scalability_results"
simulator_path = f"{base_path}/atlas-grid-simulator"

# Create directories if they don't exist
os.makedirs(config_dir, exist_ok=True)
os.makedirs(results_dir, exist_ok=True)

# Load base configuration
with open(base_config_path, 'r') as f:
    base_config = json.load(f)

# Initialize results storage
results = []

print("ATLAS Grid Simulator Scalability Test")
print("=" * 50)
print(f"Testing with 1 to {len(sites)} sites")
print(f"Jobs per site: {JOBS_PER_SITE:,}")
print(f"Base config loaded from: {base_config_path}")
print(f"Results will be saved to: {results_dir}")
print(f"Config files will be saved to: {config_dir}")
print()

# Loop through different numbers of sites
for num_sites in range(1, len(sites) + 1, 10):
    print(f"Testing with {num_sites} site(s)...")
    
    # Create modified configs
    config = base_config.copy()
    current_sites = sites[:num_sites]
    
    # Update config fields with new job count
    config["Sites"] = current_sites
    config["Input_Job_CSV"] = f"/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/data/pmbs/jan_{num_sites * JOBS_PER_SITE}_{num_sites}.csv"
    config["Output_DB"] = f"/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/output/scalability_test_{num_sites}_sites.db"
    config["Num_of_Jobs"] = num_sites * JOBS_PER_SITE
    
    # Save modified config
    config_filename = f"config_{num_sites}_sites.json"
    config_path = os.path.join(config_dir, config_filename)
    
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=4)
    
    # Check if input CSV exists
    if not os.path.exists(config["Input_Job_CSV"]):
        print(f"  ⚠️  Warning: Input CSV not found: {config['Input_Job_CSV']}")
        print(f"     Skipping {num_sites} sites test")
        continue
    
    # Run simulator and measure time
    print(f"  📁 Config: {config_filename}")
    print(f"  📊 Input CSV: jan_{num_sites * JOBS_PER_SITE}_{num_sites}.csv")
    print(f"  🏢 Total jobs: {num_sites * JOBS_PER_SITE:,}")
    print(f"  🚀 Running simulator...")
    
    start_time = time.time()
    
    try:
        # Run the simulator
        cmd = [simulator_path, "-c", config_path]
        result = subprocess.run(cmd, 
                              capture_output=True, 
                              text=True, 
                              timeout=3600)  # 1 hour timeout
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        if result.returncode == 0:
            status = "SUCCESS"
            print(f"  ✅ Completed in {execution_time:.2f} seconds")
        else:
            status = "FAILED"
            print(f"  ❌ Failed after {execution_time:.2f} seconds")
            print(f"     Error: {result.stderr[:200]}")
        
    except subprocess.TimeoutExpired:
        end_time = time.time()
        execution_time = end_time - start_time
        status = "TIMEOUT"
        print(f"  ⏰ Timeout after {execution_time:.2f} seconds")
    
    except Exception as e:
        end_time = time.time()
        execution_time = end_time - start_time
        status = "ERROR"
        print(f"  💥 Error: {str(e)}")
    
    # Store results
    result_entry = {
        'timestamp': datetime.now().isoformat(),
        'num_sites': num_sites,
        'sites': current_sites,
        'num_jobs': num_sites * JOBS_PER_SITE,
        'jobs_per_site': JOBS_PER_SITE,
        'execution_time_seconds': execution_time,
        'status': status,
        'config_file': config_filename,
        'input_csv': f"jan_{num_sites * JOBS_PER_SITE}_{num_sites}.csv"
    }
    
    results.append(result_entry)
    print()

# Save results to CSV and JSON
print("=" * 50)
print("Scalability Test Complete!")
print()

# Create results summary
results_df = pd.DataFrame(results)
timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
csv_path = os.path.join(results_dir, f"scalability_results_{timestamp_str}.csv")
json_path = os.path.join(results_dir, f"scalability_results_{timestamp_str}.json")

results_df.to_csv(csv_path, index=False)
with open(json_path, 'w') as f:
    json.dump(results, f, indent=4)

print(f"📊 Results saved to:")
print(f"   CSV: {csv_path}")
print(f"   JSON: {json_path}")
print()

# Display summary statistics
successful_runs = results_df[results_df['status'] == 'SUCCESS']
if len(successful_runs) > 0:
    print("📈 Performance Summary (Successful runs only):")
    print(f"   • Tests completed: {len(successful_runs)}/{len(results)}")
    print(f"   • Fastest execution: {successful_runs['execution_time_seconds'].min():.2f}s ({successful_runs.loc[successful_runs['execution_time_seconds'].idxmin(), 'num_sites']} sites)")
    print(f"   • Slowest execution: {successful_runs['execution_time_seconds'].max():.2f}s ({successful_runs.loc[successful_runs['execution_time_seconds'].idxmax(), 'num_sites']} sites)")
    print(f"   • Average execution time: {successful_runs['execution_time_seconds'].mean():.2f}s")
    print(f"   • Total test duration: {results_df['execution_time_seconds'].sum():.2f}s")
else:
    print("❌ No successful runs completed")

# Create scalability plots
print()
print("📈 Creating scalability plots...")

# Set up plotting style
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

# Create figure with subplots
fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))
fig.suptitle(f'ATLAS Grid Simulator Scalability Analysis\n{JOBS_PER_SITE:,} jobs per site', fontsize=16, fontweight='bold')

# Plot 1: Execution Time vs Number of Sites
successful_data = results_df[results_df['status'] == 'SUCCESS']
if len(successful_data) > 0:
    ax1.plot(successful_data['num_sites'], successful_data['execution_time_seconds'], 
             'o-', linewidth=2, markersize=8, label='Successful runs')
    ax1.set_xlabel('Number of Sites')
    ax1.set_ylabel('Execution Time (seconds)')
    ax1.set_title('Execution Time vs Number of Sites')
    ax1.grid(True, alpha=0.3)
    ax1.legend()

# Plot 2: Execution Time vs Total Jobs
if len(successful_data) > 0:
    ax2.plot(successful_data['num_jobs'], successful_data['execution_time_seconds'], 
             's-', linewidth=2, markersize=8, color='orange', label='Successful runs')
    ax2.set_xlabel('Total Number of Jobs')
    ax2.set_ylabel('Execution Time (seconds)')
    ax2.set_title('Execution Time vs Total Jobs')
    ax2.grid(True, alpha=0.3)
    ax2.legend()
    # Format x-axis to show job numbers in thousands/millions
    ax2.ticklabel_format(style='scientific', axis='x', scilimits=(0,0))

# Plot 3: Throughput (Jobs per Second)
if len(successful_data) > 0:
    successful_data_copy = successful_data.copy()
    successful_data_copy['jobs_per_second'] = successful_data_copy['num_jobs'] / successful_data_copy['execution_time_seconds']
    ax3.plot(successful_data_copy['num_sites'], successful_data_copy['jobs_per_second'], 
             '^-', linewidth=2, markersize=8, color='green', label='Jobs/second')
    ax3.set_xlabel('Number of Sites')
    ax3.set_ylabel('Jobs per Second')
    ax3.set_title('Throughput vs Number of Sites')
    ax3.grid(True, alpha=0.3)
    ax3.legend()

# Plot 4: Status Distribution
status_counts = results_df['status'].value_counts()
colors = ['green' if status == 'SUCCESS' else 'red' if status == 'FAILED' else 'orange' for status in status_counts.index]
ax4.pie(status_counts.values, labels=status_counts.index, autopct='%1.1f%%', 
        colors=colors, startangle=90)
ax4.set_title('Test Status Distribution')

plt.tight_layout()

# Save the plot
plot_path = os.path.join(results_dir, f"scalability_plot_{timestamp_str}.png")
plt.savefig(plot_path, dpi=300, bbox_inches='tight')
print(f"   📊 Plot saved to: {plot_path}")

# Show the plot
plt.show()

# Create additional detailed analysis plot if we have successful data
if len(successful_data) > 0:
    plt.figure(figsize=(12, 8))
    
    # Create scatter plot with trend line
    plt.subplot(2, 1, 1)
    plt.scatter(successful_data['num_sites'], successful_data['execution_time_seconds'], 
                s=100, alpha=0.7, c=successful_data['num_jobs'], cmap='viridis')
    plt.colorbar(label='Total Jobs')
    
    # Add trend line
    z = np.polyfit(successful_data['num_sites'], successful_data['execution_time_seconds'], 1)
    p = np.poly1d(z)
    plt.plot(successful_data['num_sites'], p(successful_data['num_sites']), "r--", alpha=0.8, linewidth=2)
    
    plt.xlabel('Number of Sites')
    plt.ylabel('Execution Time (seconds)')
    plt.title(f'Scalability Analysis - {JOBS_PER_SITE:,} jobs per site')
    plt.grid(True, alpha=0.3)
    
    # Scaling efficiency plot
    plt.subplot(2, 1, 2)
    if len(successful_data) > 1:
        baseline_time = successful_data['execution_time_seconds'].iloc[0]
        baseline_sites = successful_data['num_sites'].iloc[0]
        
        ideal_scaling = baseline_time * (successful_data['num_sites'] / baseline_sites)
        scaling_efficiency = (ideal_scaling / successful_data['execution_time_seconds']) * 100
        
        plt.plot(successful_data['num_sites'], scaling_efficiency, 'o-', linewidth=2, markersize=8)
        plt.axhline(y=100, color='red', linestyle='--', alpha=0.7, label='Perfect scaling')
        plt.xlabel('Number of Sites')
        plt.ylabel('Scaling Efficiency (%)')
        plt.title('Scaling Efficiency (Higher is Better)')
        plt.grid(True, alpha=0.3)
        plt.legend()
    
    plt.tight_layout()
    
    # Save detailed analysis plot
    detailed_plot_path = os.path.join(results_dir, f"detailed_scalability_analysis_{timestamp_str}.png")
    plt.savefig(detailed_plot_path, dpi=300, bbox_inches='tight')
    print(f"   📊 Detailed analysis plot saved to: {detailed_plot_path}")
    plt.show()

print()
print("🔍 Analysis complete! Check the generated plots and CSV file for detailed scalability insights.")
print(f"📈 Total jobs tested range: {results_df['num_jobs'].min():,} to {results_df['num_jobs'].max():,}")