import pandas as pd

#load data
df = pd.read_csv('books.csv')

#display basic info
print(f"df.describe: \n{df.describe()}\n\n")
print(f"df info: \n{df.info()}\n\n")
print(f"df head: \n{df.head()}\n\n")

#remove duplicate row
df.drop_duplicates(inplace=True)

#convert availability column to binary format
df['availability'] = df['availability'].map({'In stock': 1, 'Out of stock': 0})

df['price'] = pd.to_numeric(df['price'], errors='coerce')

#check for missing values
missing_values = df.isnull().sum()
print("Missing values per column:\n", missing_values)

has_missing_values = df.isnull().values.any()
print("Has missing values:", has_missing_values)

total_missing = df.isnull().sum().sum()
print("Total missing values:", total_missing)

#export cleaned data
df.to_csv('cleaned_data.csv', index=False)