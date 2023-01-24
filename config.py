import os

spark_master = "spark://spark-master:7077"

#cwd = os.path.dirname(os.path.realpath(__file__))
cwd = os.getcwd()
DATA_DIR = f"{cwd}/datasets"
RAW_FILE_PATH = f"{DATA_DIR}/raw_data.zip"
GOOGLE_DRIVE_URL = "https://drive.google.com/file/d/1ZNoIaUXNDVf9PX--G-ANUEjuj318XtmK/view?usp=share_link"

parquets_path = f"{DATA_DIR}/intermid_parquets"
outcomes_path = f"{parquets_path}/outcome/*"
places_path = f"{parquets_path}/places/*"

merged_data_path = f"{DATA_DIR}/merged_data"
stats_path = f"{DATA_DIR}/statistics"