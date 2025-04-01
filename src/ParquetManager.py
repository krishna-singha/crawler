import pandas as pd
import os
from config.config import DATA_DIR
from filelock import FileLock
import json

class ParquetManager:
    def __init__(self, file_path):
        self.file_path = file_path
        self.lock_path = f"{file_path}.lock"
        self.check_dir_file()

    def check_dir_file(self):
        """Ensure the data directory and Parquet file exist."""
        os.makedirs(DATA_DIR, exist_ok=True)
        if not os.path.exists(self.file_path):
            empty_df = pd.DataFrame(columns=["url", "favicon", "title", "headings", "content", "filters", "timestamp"])
            empty_df.to_parquet(self.file_path, engine="pyarrow", compression="snappy")
            print("✅ Created empty Parquet file.")

    def _read_data(self):
        """Read data from Parquet file if it exists, otherwise return an empty DataFrame."""
        if os.path.exists(self.file_path):
            try:
                return pd.read_parquet(self.file_path, engine="pyarrow")
            except Exception as e:
                print(f"⚠️ Error reading Parquet file: {e}. Resetting file.")
                self.check_dir_file()
        return pd.DataFrame(columns=["url", "favicon", "title", "headings", "content", "filters", "timestamp"])

    def write_data(self, new_data):
        """Append new data only if it doesn't already exist in the Parquet file."""
        if new_data.empty:
            print("⚠️ No data to write. Skipping operation.")
            return

        with FileLock(self.lock_path):  # Prevent concurrent writes
            df_existing = self._read_data()
            df_combined = pd.concat([df_existing, new_data], ignore_index=True)
            
            # Drop duplicates based on URL
            df_combined.drop_duplicates(subset=["url"], keep="first", inplace=True)
            
            if len(df_combined) == len(df_existing):
                print("✅ Data already exists. Skipping write operation.")
                return
            
            df_combined.to_parquet(self.file_path, engine="pyarrow", compression="snappy")
            print("✅ Data written successfully.")

# # Example Usage
# if __name__ == "__main__":
#     FILE_PATH = os.path.join(DATA_DIR, "data.parquet")
#     pm = ParquetManager(FILE_PATH)
#     new_data = pd.DataFrame([{
#         "url": "https://www.example.com",
#         "favicon": "https://www.example.com/favicon.ico",
#         "title": "Example Domain",
#         "headings": json.dumps(["Heading 1", "Heading 2"]),
#         "content": json.dumps(["Content paragraph 1", "Content paragraph 2"]),
#         "filters": "all",
#         "timestamp": pd.Timestamp.now()
#     }])
#     pm.write_data(new_data)  # Writes only if new_data isn't already in the file
#     df = pm._read_data()
#     print(df)