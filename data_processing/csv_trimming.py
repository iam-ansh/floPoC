import pandas as pd
from pathlib import Path

def trim_csv():
    csv_folder = Path("csv_test")
    for file in csv_folder.glob("*.csv"):
        df = pd.read_csv(file)

        filtered = df[df["profile_id"] == 0]
        filtered.to_csv(file, index=False)

trim_csv()