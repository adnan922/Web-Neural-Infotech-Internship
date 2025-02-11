import sqlite3
import pandas as pd
from datetime import datetime

# Read the cleaned CSV files
df_2024 = pd.read_csv('cleaned_india_companies_2024.csv')
df_2023 = pd.read_csv('cleaned_india_companies_2023.csv')

# Create SQLite connection
conn = sqlite3.connect('india_companies.db')
cursor = conn.cursor()

# Create tables
def create_tables():
    # Companies table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS companies (
        company_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        industry TEXT,
        headquarters TEXT,
        UNIQUE(name)
    )''')

    # Financial data table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS financial_data (
        financial_id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER,
        year INTEGER,
        rank INTEGER,
        revenue REAL,
        profit REAL,
        assets REAL,
        market_value REAL,
        revenue_growth REAL,
        state_controlled BOOLEAN,
        forbes_rank INTEGER,
        FOREIGN KEY (company_id) REFERENCES companies(company_id),
        UNIQUE(company_id, year)
    )''')

    # Industry categories table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS industry_categories (
        industry_id INTEGER PRIMARY KEY AUTOINCREMENT,
        industry_name TEXT UNIQUE
    )''')

    # Cities table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS cities (
        city_id INTEGER PRIMARY KEY AUTOINCREMENT,
        city_name TEXT UNIQUE,
        tier TEXT
    )''')

    conn.commit()

def insert_company(name, industry, headquarters):
    cursor.execute('''
    INSERT OR IGNORE INTO companies (name, industry, headquarters)
    VALUES (?, ?, ?)
    ''', (name, industry, headquarters))
    
    cursor.execute('SELECT company_id FROM companies WHERE name = ?', (name,))
    return cursor.fetchone()[0]

def insert_financial_data(company_id, data, year):
    cursor.execute('''
    INSERT OR REPLACE INTO financial_data 
    (company_id, year, rank, revenue, profit, assets, market_value, 
     revenue_growth, state_controlled, forbes_rank)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        company_id,
        year,
        data.get('Rank'),
        data.get('Revenue'),
        data.get('Profit'),
        data.get('Assets'),
        data.get('Value'),
        data.get('Revenue_Growth'),
        data.get('State_Controlled', False),
        data.get('Forbes_Rank')
    ))

def populate_database():
    # Process 2024 data
    for _, row in df_2024.iterrows():
        company_id = insert_company(row['Name'], row['Industry'], row['Headquarters'])
        insert_financial_data(company_id, row, 2024)

    # Process 2023 data
    for _, row in df_2023.iterrows():
        company_id = insert_company(row['Name'], row['Industry'], row['Headquarters'])
        insert_financial_data(company_id, row, 2023)

    conn.commit()

# Create useful views
def create_views():
    # Year-over-year comparison view
    cursor.execute('''
    CREATE VIEW IF NOT EXISTS yoy_comparison AS
    SELECT 
        c.name,
        c.industry,
        c.headquarters,
        f1.year as year_2024,
        f1.revenue as revenue_2024,
        f1.profit as profit_2024,
        f2.year as year_2023,
        f2.revenue as revenue_2023,
        f2.profit as profit_2023,
        ((f1.revenue - f2.revenue) / f2.revenue * 100) as revenue_growth,
        ((f1.profit - f2.profit) / ABS(f2.profit) * 100) as profit_growth
    FROM companies c
    JOIN financial_data f1 ON c.company_id = f1.company_id AND f1.year = 2024
    JOIN financial_data f2 ON c.company_id = f2.company_id AND f2.year = 2023
    ''')

    # Industry summary view
    cursor.execute('''
    CREATE VIEW IF NOT EXISTS industry_summary AS
    SELECT 
        c.industry,
        f.year,
        COUNT(*) as company_count,
        SUM(f.revenue) as total_revenue,
        SUM(f.profit) as total_profit,
        AVG(f.revenue) as avg_revenue,
        AVG(f.profit) as avg_profit
    FROM companies c
    JOIN financial_data f ON c.company_id = f.company_id
    GROUP BY c.industry, f.year
    ''')

    conn.commit()

# Example queries function
def create_example_queries():
    return {
        "Top 10 companies by revenue 2024": '''
            SELECT c.name, f.revenue, f.profit, c.industry
            FROM companies c
            JOIN financial_data f ON c.company_id = f.company_id
            WHERE f.year = 2024
            ORDER BY f.revenue DESC
            LIMIT 10
        ''',
        
        "Most profitable industries": '''
            SELECT 
                industry,
                year,
                total_profit,
                avg_profit
            FROM industry_summary
            ORDER BY total_profit DESC
        ''',
        
        "Companies with highest revenue growth": '''
            SELECT 
                name,
                industry,
                revenue_growth,
                revenue_2024,
                revenue_2023
            FROM yoy_comparison
            ORDER BY revenue_growth DESC
            LIMIT 10
        '''
    }

# Initialize database
def initialize_database():
    create_tables()
    populate_database()
    create_views()
    
    # Run example queries
    queries = create_example_queries()
    print("\nExample query results:")
    for query_name, query in queries.items():
        print(f"\n{query_name}:")
        result = pd.read_sql_query(query, conn)
        print(result)

# Run the initialization
if __name__ == "__main__":
    initialize_database()
    
    # Close connection
    conn.close()

print("Database created successfully with example queries executed.")