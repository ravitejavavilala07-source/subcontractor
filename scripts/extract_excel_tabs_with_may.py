import pandas as pd
import json
from pathlib import Path

input_folder = Path("data/input/may_2025")  # Adjust if needed
all_data = {}

for file_path in input_folder.glob("*.xlsx"):
    file_name = file_path.name
    file_entry = {}
    try:
        xls = pd.ExcelFile(file_path)
        for sheet_name in xls.sheet_names:
            try:
                df = xls.parse(sheet_name)
                sample = df.head(5).fillna('').astype(str).to_dict(orient="records")
                columns = df.columns.astype(str).tolist()
                file_entry[sheet_name] = {
                    "columns": columns,
                    "sample_rows": sample
                }
            except Exception as e:
                file_entry[sheet_name] = {"error": str(e)}
    except Exception as e:
        file_entry["error"] = str(e)
    all_data[file_name] = file_entry

# Save the structured JSON
output_json_path = input_folder / "all_excel_tabs_with_may.json"
with open(output_json_path, "w") as f:
    json.dump(all_data, f, indent=2)

print(f"✅ JSON saved at: {output_json_path}")

