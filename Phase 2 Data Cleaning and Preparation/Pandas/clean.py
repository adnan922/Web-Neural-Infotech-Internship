
import pandas as pd
import numpy as np

# Function to convert crore to billions USD (assuming 1 USD = 83 INR)
def crore_to_billion_usd(value):
    if pd.isna(value):
        return value
    # Handle string values
    if isinstance(value, str):
        # Replace various forms of minus signs and remove commas
        value = value.replace('−', '-').replace(',', '')
        try:
            value = float(value)
        except ValueError:
            return np.nan
    # Convert crore to billion USD
    return round((value / 100) / 83, 2)  # 1 crore = 10M INR, then convert to USD

# Function to clean percentage strings
def clean_percentage(value):
    if pd.isna(value):
        return value
    if isinstance(value, str):
        value = value.strip('%')
        try:
            return float(value)
        except ValueError:
            return np.nan
    return value

# Read the 2024 data
df_2024 = pd.read_csv('Largest Companies in India 2024 Forbes.csv')

# Read the 2023 data
df_2023 = pd.read_csv('Largest Companies in India 2023 Forbes.csv')

# Clean 2024 data
def clean_2024_data(df):
    #rename columns to simpler names
    df = df.rename(columns={
        'Revenue(billions US$)': 'Revenue',
        'Profit(billions US$)': 'Profit',
        'Assets(billions US$)': 'Assets',
        'Value(billions US$)': 'Value',
        'Forbes 2000 rank': 'Forbes_Rank'
    })
    
    #convert numeric columns
    numeric_columns = ['Revenue', 'Profit', 'Assets', 'Value']
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    #clean up Headquarters and Industry
    df['Headquarters'] = df['Headquarters'].str.strip()
    df['Industry'] = df['Industry'].str.strip()
    
    #add year 
    df['Year'] = 2024
    
    return df

# Clean 2023 data
def clean_2023_data(df):
    #convert revenue and profits afrom crore to billon USD
    df['Revenue'] = df['Revenue(in  ₹ Crore)'].apply(crore_to_billion_usd)
    df['Profit'] = df['Profits(in  ₹ Crore)'].apply(crore_to_billion_usd)
    
    #clean aRevenue growth percentages
    df['Revenue_Growth'] = df['Revenue growth'].apply(clean_percentage)
    
    #clean up Headquarters and Industry
    df['Headquarters'] = df['Headquarters'].str.strip()
    df['Industry'] = df['Industry'].str.strip()
    
    #add year identifier
    df['Year'] = 2023
    
    # add State Controlled indcator
    df['State_Controlled'] = df['State Controlled'].fillna('No')
    df['State_Controlled'] = df['State_Controlled'].map({'Yes': True, 'No': False})
    
    # Select and reorder columns
    columns = ['Rank', 'Name', 'Industry', 'Revenue', 'Profit', 'Revenue_Growth', 
              'Headquarters', 'State_Controlled', 'Year']
    
    return df[columns]

# Clean both datasets
df_2024_clean = clean_2024_data(df_2024)
df_2023_clean = clean_2023_data(df_2023)

# Create combined dataset for comparison
def prepare_for_comparison(df_2024, df_2023):
    #select common columns
    common_columns = ['Rank', 'Name', 'Industry', 'Revenue', 'Profit', 
                     'Headquarters', 'Year']
    
    df_2024_comp = df_2024[common_columns].copy()
    df_2023_comp = df_2023[common_columns].copy()
    
    # Combine datasets
    combined_df = pd.concat([df_2024_comp, df_2023_comp])
    
    return combined_df

# Create combined dataset
combined_df = prepare_for_comparison(df_2024_clean, df_2023_clean)

# Basic analysis
print("2024 Dataset Info:")
print(df_2024_clean.info())
print("\n2023 Dataset Info:")
print(df_2023_clean.info())

#display sample of cleaned data
print("\nSample of 2024 cleaned data:")
print(df_2024_clean.head())
print("\nSample of 2023 cleaned data:")
print(df_2023_clean.head())

# Save cleaned datasets
df_2024_clean.to_csv('cleaned_india_companies_2024.csv', index=False)
df_2023_clean.to_csv('cleaned_india_companies_2023.csv', index=False)
combined_df.to_csv('combined_india_companies_2023_2024.csv', index=False)

#more analysis funtions
def get_year_over_year_changes(combined_df):
    """Calculate year-over-year changes for companies"""
    # change the data to compare years
    changes = combined_df.pivot(index='Name', 
                              columns='Year',
                              values=['Revenue', 'Profit', 'Rank'])
    
    # Calculate changes
    changes['Revenue_Change'] = changes[('Revenue', 2024)] - changes[('Revenue', 2023)]
    changes['Profit_Change'] = changes[('Profit', 2024)] - changes[('Profit', 2023)]
    changes['Rank_Change'] = changes[('Rank', 2023)] - changes[('Rank', 2024)]
    
    return changes.sort_values('Revenue_Change', ascending=False)

# Print summary of year-over-year changes
print("\nYear-over-year changes summary:")
yoy_changes = get_year_over_year_changes(combined_df)
print(yoy_changes.head())