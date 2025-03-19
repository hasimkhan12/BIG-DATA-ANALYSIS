import dask.dataframe as dd

# Load your CSV file (replace with your actual filename)
file_path = r"C:\Users\vaish\OneDrive\Documents\netflix_titles.csv\netflix_titles.csv"
df = dd.read_csv(r"C:\Users\vaish\OneDrive\Documents\netflix_titles.csv\netflix_titles.csv")

# Display column names and first few rows
print("Columns in dataset:", df.columns)
print(df.head())

# Check missing values
missing_values = df.isnull().sum().compute()
print("Missing values in dataset:\n", missing_values)

# Drop rows with missing values
df_cleaned = df.dropna()

# Convert release_year to integer
df_cleaned['release_year'] = df_cleaned['release_year'].astype(int)

# Example: Filter movies released after 2015
df_filtered = df_cleaned[df_cleaned['release_year'] > 2015]

# Group by 'type' (e.g., Movies vs TV Shows)
result = df_filtered.groupby('type').size().compute()
print("Count by type:\n", result)

# Save cleaned data to a new CSV file
df_filtered.to_csv("processed_data.csv", single_file=True)

print("Processing complete! 🚀")
