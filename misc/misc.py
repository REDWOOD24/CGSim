import pandas as pd
import os

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

# Read the main dataset
df_ALL = pd.read_csv('/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/data/jan_10k_by_site.csv')

# Create the output directory if it doesn't exist
output_dir = '/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/data/pmbs/'
os.makedirs(output_dir, exist_ok=True)

print(f"Total sites available: {len(sites)}")
print(f"Output directory: {output_dir}")
print("Creating CSV files...")

# Loop through increasing number of sites (1 to total number of sites)
for num_sites in range(1, len(sites) + 1):
    # Get the first num_sites sites
    current_sites = sites[:num_sites]
    
    # Filter dataframe for current sites
    df_filtered = df_ALL[df_ALL['computingsite'].isin(current_sites)]
    
    # Sample up to 1000 records per site
    df_sampled = df_filtered.groupby('computingsite').apply(
        lambda x: x.sample(n=min(200, len(x)), random_state=42)
    ).reset_index(drop=True)
    
    # Create filename based on the pattern jan_{num_sites*1000}_{num_sites}.csv
    theoretical_records = num_sites * 200
    filename = f'jan_{theoretical_records}_{num_sites}.csv'
    filepath = os.path.join(output_dir, filename)
    
    # Save to CSV
    df_sampled.to_csv(filepath, index=False)
    
    # Print progress
    actual_records = len(df_sampled)
    print(f"✓ Created {filename} - {actual_records} records from {num_sites} sites")

print(f"\nCompleted! Created {len(sites)} CSV files in {output_dir}")