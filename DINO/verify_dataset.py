from pathlib import Path
import pandas as pd

# Paths
DATA_DIR = Path(__file__).parent / "vehicle_damage_data" / "image"
CSV_PATH = Path(__file__).parent / "vehicle_damage_data" / "data.csv"

def main():
    print("Checking dataset paths...")
    print("Images dir:", DATA_DIR.resolve())
    print("CSV path  :", CSV_PATH.resolve())

    if not DATA_DIR.exists():
        print("ERROR: image directory not found.")
        return
    if not CSV_PATH.exists():
        print("ERROR: CSV file not found.")
        return

    df = pd.read_csv(CSV_PATH)
    print("CSV columns:", list(df.columns))

    # Expect columns 'image' and 'classes'
    if not {"image", "classes"}.issubset(df.columns):
        print("ERROR: CSV missing 'image' or 'classes' column.")
        return

    label_map = dict(zip(df["image"].astype(str), df["classes"].astype(str)))
    files_on_disk = {p.name for p in DATA_DIR.iterdir() if p.is_file()}
    files_in_csv = set(label_map.keys())

    print(f"Rows in CSV          : {len(df)}")
    print(f"Images on disk       : {len(files_on_disk)}")
    print(f"In CSV only          : {len(files_in_csv - files_on_disk)}")
    print(f"On disk not in CSV   : {len(files_on_disk - files_in_csv)}")

    print("\nSample mappings:")
    for i, (fname, label) in enumerate(label_map.items()):
        if i >= 10:
            break
        print(f"  {fname} -> {label}")

if __name__ == "__main__":
    main()