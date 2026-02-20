import pandas as pd
import numpy as np

# Simulate the data
df = pd.DataFrame({
    'first_name': ['John', 'Jane', np.nan, 'mary', 'Robert'],
    'last_name': ['Doe', 'Smith', 'Johnson', 'brown', np.nan]
})

# Logic from dataprofiler.py
# 3. Name Casing
bad_casing = df[~df['first_name'].astype(str).str.istitle() & df['first_name'].notna()]
print("Bad casing for first_name:")
print(bad_casing)

bad_casing_last = df[~df['last_name'].astype(str).str.istitle() & df['last_name'].notna()]
print("\nBad casing for last_name:")
print(bad_casing_last)
