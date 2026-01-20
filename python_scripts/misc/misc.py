import pandas as pd

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
df_ALL = pd.read_csv('/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/data/jan_10k_by_site.csv')
# sites = ["MPPMU"]
df_filtered = df_ALL[df_ALL['computingsite'].isin(sites)]
# # for site in df_filtered['computingsite'].unique():
# # 	df_site = df_filtered[df_filtered['computingsite'] == site].head(2000)
# # 	df_site.to_csv(f'ATLAS-GRID-SIMULATION/data/jan_1k_by_{site}.csv', index=False)
df_sampled = df_filtered.groupby('computingsite').apply(lambda x: x.sample(n=min(200, len(x)), random_state=42)).reset_index(drop=True)
df_sampled.to_csv('/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/data/jan_200_selected_may.csv', index=False)
# for site in sites:
	
# 	df = df_ALL[df_ALL['computingsite']==site]

# 	df.to_csv(f'/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/data/jan_10k_by_{site}.csv', index=False)