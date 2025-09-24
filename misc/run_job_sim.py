import json
import subprocess
import time
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime

# Use single site for testing (first site from the list)
test_site = "NET2_Amherst"

# Job counts to test
job_counts = [10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000]

# Base paths
base_path = "/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/build"
base_config_path = "/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/config-files/config_base.json"
config_dir = f"{base_path}/config-files/job_scalability_configs"
results_dir = f"{base_path}/job_scalability_results"
simulator_path = f"{base_path}/atlas-grid-simulator"

# Create directories if they don't exist
os.makedirs(config_dir, exist_ok=True)
os.makedirs(results_dir, exist_ok=True)

# Load base configuration
with open(base_config_path, 'r') as f:
    base_config = json.load(f)

# Initialize results storage
results = []

print("ATLAS Grid Simulator Job Scalability Test")
print("=" * 50)
print(f"Testing single site: {test_site}")
print(f"Using job file: jan_10k_by_BNL (BNL jobs only)")
print(f"Job counts: {job_counts}")
print(f"Base config loaded from: {base_config_path}")
print(f"Results will be saved to: {results_dir}")
print(f"Config files will be saved to: {config_dir}")
print()

# Loop through different numbers of jobs
for num_jobs in job_counts:
    print(f"Testing with {num_jobs} job(s)...")
    
    # Create modified config
    config = base_config.copy()
    
    # Update config fields for single site (BNL only) with varying job count
    config["Sites"] = [test_site]  # Only BNL site
    config["Input_Job_CSV"] = "/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/data/debugscalability/NET2_jobs.csv"
    config["Output_DB"] = f"/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/output/job_scalability_test_{num_jobs}_jobs.db"
    config["Num_of_Jobs"] = num_jobs
    
    # Save modified config
    config_filename = f"config_{num_jobs}_jobs.json"
    config_path = os.path.join(config_dir, config_filename)
    
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=4)
    
    # Check if input CSV exists
    if not os.path.exists(config["Input_Job_CSV"]):
        print(f"  ⚠️  Warning: Input CSV not found: {config['Input_Job_CSV']}")
        print(f"     Skipping {num_jobs} jobs test")
        continue
    
    # Run simulator and measure time
    print(f"  📁 Config: {config_filename}")
    print(f"  📊 Input CSV: NET2_jobs.csv")
    print(f"  🏢 Site: {test_site}")
    print(f"  📝 Processing {num_jobs} jobs from file")
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
        'test_site': test_site,
        'num_sites': 1,
        'num_jobs': num_jobs,
        'execution_time_seconds': execution_time,
        'status': status,
        'config_file': config_filename,
        'input_csv': 'jan_10k_by_BNL'
    }
    
    results.append(result_entry)
    print()

# Save results to CSV and JSON
print("=" * 50)
print("Job Scalability Test Complete!")
print()

# Create results summary
results_df = pd.DataFrame(results)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
csv_path = os.path.join(results_dir, f"job_scalability_results_{timestamp}.csv")
json_path = os.path.join(results_dir, f"job_scalability_results_{timestamp}.json")

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
    print(f"   • Site tested: {test_site}")
    print(f"   • Job counts tested: {sorted(successful_runs['num_jobs'].tolist())}")
    print(f"   • Fastest execution: {successful_runs['execution_time_seconds'].min():.2f}s ({successful_runs.loc[successful_runs['execution_time_seconds'].idxmin(), 'num_jobs']} jobs)")
    print(f"   • Slowest execution: {successful_runs['execution_time_seconds'].max():.2f}s ({successful_runs.loc[successful_runs['execution_time_seconds'].idxmax(), 'num_jobs']} jobs)")
    print(f"   • Average execution time: {successful_runs['execution_time_seconds'].mean():.2f}s")
    print(f"   • Total test duration: {results_df['execution_time_seconds'].sum():.2f}s")
    
    # Performance analysis
    if len(successful_runs) > 1:
        print()
        print("⚡ Performance Analysis:")
        jobs_per_second = successful_runs['num_jobs'] / successful_runs['execution_time_seconds']
        print(f"   • Average jobs/second: {jobs_per_second.mean():.2f}")
        print(f"   • Best jobs/second: {jobs_per_second.max():.2f} ({successful_runs.loc[jobs_per_second.idxmax(), 'num_jobs']} jobs)")
        print(f"   • Worst jobs/second: {jobs_per_second.min():.2f} ({successful_runs.loc[jobs_per_second.idxmin(), 'num_jobs']} jobs)")
        
        # Check for performance scaling
        correlation = successful_runs['num_jobs'].corr(successful_runs['execution_time_seconds'])
        print(f"   • Job count vs execution time correlation: {correlation:.3f}")
        if correlation > 0.8:
            print("     → Strong linear scaling relationship")
        elif correlation > 0.5:
            print("     → Moderate scaling relationship")
        else:
            print("     → Weak or non-linear scaling relationship")

    print()
    print("📊 Generating Job Scalability Plot...")
    
    # Create the job scalability plot
    plt.figure(figsize=(10, 6))
    
    # Sort data by number of jobs for proper line plotting
    plot_data = successful_runs.sort_values('num_jobs')
    
    # Plot the main scaling curve
    plt.plot(plot_data['num_jobs'], plot_data['execution_time_seconds'], 
             'bo-', linewidth=2, markersize=6, label='Execution Time')
    
    # Add grid
    plt.grid(True, alpha=0.3)
    
    # Set labels and title
    plt.xlabel('Number of Jobs', fontsize=12)
    plt.ylabel('Time (Seconds)', fontsize=12)
    plt.title('Job Scaling', fontsize=14, fontweight='bold')
    
    # Format axes
    plt.ticklabel_format(style='plain', axis='both')
    
    # Add some styling to match the reference image
    plt.gca().spines['top'].set_visible(False)
    plt.gca().spines['right'].set_visible(False)
    
    # Adjust layout to prevent label cutoff
    plt.tight_layout()
    
    # Save the plot
    plot_path = os.path.join(results_dir, f"job_scalability_plot_{timestamp}.png")
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    
    print(f"📈 Scalability plot saved to: {plot_path}")
    
    # Show the plot if running interactively
    # plt.show()  # Uncomment if you want to display the plot
    
    # Optional: Create additional plots for deeper analysis
    if len(successful_runs) > 3:
        # Jobs per second plot
        plt.figure(figsize=(10, 6))
        jobs_per_sec = plot_data['num_jobs'] / plot_data['execution_time_seconds']
        plt.plot(plot_data['num_jobs'], jobs_per_sec, 
                'ro-', linewidth=2, markersize=6, label='Jobs/Second')
        plt.grid(True, alpha=0.3)
        plt.xlabel('Number of Jobs', fontsize=12)
        plt.ylabel('Jobs per Second', fontsize=12)
        plt.title('Processing Rate Scaling', fontsize=14, fontweight='bold')
        plt.gca().spines['top'].set_visible(False)
        plt.gca().spines['right'].set_visible(False)
        plt.tight_layout()
        
        throughput_plot_path = os.path.join(results_dir, f"job_throughput_plot_{timestamp}.png")
        plt.savefig(throughput_plot_path, dpi=300, bbox_inches='tight')
        print(f"📈 Throughput plot saved to: {throughput_plot_path}")
        
        # Log-log plot to identify scaling patterns
        plt.figure(figsize=(10, 6))
        plt.loglog(plot_data['num_jobs'], plot_data['execution_time_seconds'], 
                  'go-', linewidth=2, markersize=6, label='Log-Log Scaling')
        plt.grid(True, alpha=0.3)
        plt.xlabel('Number of Jobs (log scale)', fontsize=12)
        plt.ylabel('Time (Seconds, log scale)', fontsize=12)
        plt.title('Job Scaling (Log-Log)', fontsize=14, fontweight='bold')
        plt.gca().spines['top'].set_visible(False)
        plt.gca().spines['right'].set_visible(False)
        plt.tight_layout()
        
        loglog_plot_path = os.path.join(results_dir, f"job_scaling_loglog_{timestamp}.png")
        plt.savefig(loglog_plot_path, dpi=300, bbox_inches='tight')
        print(f"📈 Log-log plot saved to: {loglog_plot_path}")
    
    plt.close('all')  # Close all plots to free memory
        
else:
    print("❌ No successful runs completed - cannot generate plots")

print()
print("🔍 Analysis complete! Check the results directory for:")
print("   📊 CSV/JSON data files")
print("   📈 Job scalability visualization(s)")
print("   💡 Use these files to identify performance bottlenecks and optimize job processing!")