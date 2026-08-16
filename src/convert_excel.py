import pandas as pd
from pathlib import Path

input_file = r"C:\Big data Engineering\spark-hive-json\data\Raw\online_retail_II.xlsx"
output_file = r"C:\Big data Engineering\spark-hive-json\data\Raw\online_retail_II.csv"
print("Reading Excel file...")

df1 = pd.read_excel(
    input_file,
    sheet_name="Year 2009-2010"
)

print("2009-2010 rows:", len(df1))

df2 = pd.read_excel(
    input_file,
    sheet_name="Year 2010-2011"
)

print("2010-2011 rows:", len(df2))

df = pd.concat([df1, df2], ignore_index=True)

print("TOTAL ROWS:", len(df))

df.to_csv(
    output_file,
    index=False
)

print("CSV CREATED SUCCESSFULLY")
print(output_file)