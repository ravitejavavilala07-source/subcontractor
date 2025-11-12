#!/usr/bin/env python3
import os
import shutil
from pathlib import Path

# Create directories if they don't exist
Path("data/input/vendor_lists").mkdir(parents=True, exist_ok=True)
Path("data/archive").mkdir(parents=True, exist_ok=True)
Path("scripts").mkdir(parents=True, exist_ok=True)

# May 2025 Input Files (KEEP in data/input/)
input_files = [
    "Copy of SmartWorks Sub Vendor Accrual - 05.31.25(accural).csv",
    "Copy of SmartWorks Sub Vendor Accrual - 05.31.25(tab2).csv",
    "Copy of SmartWorks Sub Vendor Accrual - 05.31.25(tab3).csv",
    "SW Subvendor balance Sheet 01.01.25_05.31.25.csv",
    "SW Pivot Table Service Dates 01.01.25_05.31.25.csv"
]

# Vendor Lists (Move to vendor_lists/)
vendor_lists = [
    "Cognizant Sub s-escalation list.xlsx",
    "Cognizant Sub vendor list.xlsx",
    "Cox Sub vendor list-2.xlsx",
    "Rise IT Sub Vendor List.xlsx",
    "Smartworks Sub vendor list-2.xlsx",
    "TCS Sub vendor list.xlsx",
    "TCS Subs-escalation list.xlsx",
    "Xela Sub vendor list.xlsx",
    "iTech Other Sub vendor list-iTech.xlsx"
]

# Archive Files (Old data)
archive_files = [
    "SW Pivot Table_07.01.25.csv",
    "SW Pivot Table_10.22.25.xlsx",
    "SmartWorks Sub Vendor Accrual - 05.31.25.csv",
    "SmartWorks Sub Vendor Accrual - 09.30.25.xlsx",
    "YTD_09.30.25_SW Subvendor balance sheet.xlsx",
    "Subcontractor_Accrual_Reconciliation_05.08.2025_.csv",
    "Subcontractor_Accrual_Reconciliation_05.08.2025_YTD Summary.csv",
    "Subcontractor_Accrual_Reconciliation_05.08.2025_exceptions ytab.csv",
    "Subcontractor_Accrual_Reconciliation_05.08.2025_vendor_name.csv"
]

# Delete duplicate CSV files
delete_files = [
    "Cognizant-Sub-s-escalation-list.csv",
    "Cognizant-Sub-vendor-list.csv",
    "Cox-Sub-vendor-list-2.csv",
    "Rise-IT-Sub-Vendor-List.csv",
    "Smartworks-Sub-vendor-list-2.csv",
    "TCS-Sub-vendor-list.csv",
    "TCS-Subs-escalation-list.csv",
    "Xela-Sub-vendor-list.csv",
    "iTech-Other-Sub-vendor-list-iTech.csv"
]

# Move input files
print("📥 Moving May 2025 input files...")
for file in input_files:
    if os.path.exists(file):
        shutil.move(file, f"data/input/{file}")
        print(f"  ✅ Moved: {file}")
    else:
        print(f"  ⚠️  Not found: {file}")

# Move vendor lists
print("\n📋 Moving vendor lists...")
for file in vendor_lists:
    if os.path.exists(file):
        shutil.move(file, f"data/input/vendor_lists/{file}")
        print(f"  ✅ Moved: {file}")
    else:
        print(f"  ⚠️  Not found: {file}")

# Archive old files
print("\n📦 Archiving old files...")
for file in archive_files:
    if os.path.exists(file):
        shutil.move(file, f"data/archive/{file}")
        print(f"  ✅ Archived: {file}")
    else:
        print(f"  ⚠️  Not found: {file}")

# Move script
print("\n🔧 Moving scripts...")
if os.path.exists("extract_excel_tabs_with_may.py"):
    shutil.move("extract_excel_tabs_with_may.py", "scripts/extract_excel_tabs_with_may.py")
    print("  ✅ Moved: extract_excel_tabs_with_may.py")

# Delete duplicate CSVs
print("\n🗑️  Deleting duplicate CSV files...")
for file in delete_files:
    if os.path.exists(file):
        os.remove(file)
        print(f"  ✅ Deleted: {file}")
    else:
        print(f"  ⚠️  Not found: {file}")

print("\n✅ File organization complete!")
print("\nNext steps:")
print("1. Run: tree -L 3")
print("2. Verify the structure")
print("3. Run: git status")
print("4. Commit changes to Git")
