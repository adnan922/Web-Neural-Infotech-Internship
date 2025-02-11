import mysql.connector
import csv
import os
from mysql.connector import Error

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',  
    'password': 'root@123',  
    'database': 'diabetes_db'
}

def create_diabetes_database(csv_filename):
    if not os.path.exists(csv_filename):
        print(f"\nError: File '{csv_filename}' not found in the current directory.")
        print(f"Current directory: {os.getcwd()}")
        print("\nFiles in current directory:")
        for file in os.listdir():
            print(f"- {file}")
        return False
    
    try:
        # First connect without database to create it if needed
        conn = mysql.connector.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        
        cursor = conn.cursor()
        
        # Create database if it doesn't exist
        cursor.execute('CREATE DATABASE IF NOT EXISTS diabetes_db')
        cursor.execute('USE diabetes_db')
        
        # Create table with new schema matching diabetes dataset
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS diabetes_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                gender VARCHAR(10) NOT NULL,
                age INT NOT NULL,
                hypertension TINYINT NOT NULL,
                heart_disease TINYINT NOT NULL,
                smoking_history VARCHAR(20) NOT NULL,
                bmi DECIMAL(5,2) NOT NULL,
                HbA1c_level DECIMAL(4,2) NOT NULL,
                blood_glucose_level INT NOT NULL,
                diabetes TINYINT NOT NULL
            )
        ''')
        
        # Clear existing data
        cursor.execute('TRUNCATE TABLE diabetes_data')
        
        # Read CSV file and insert data
        with open(csv_filename, 'r', encoding='utf-8') as file:
            print("\nFirst few lines of CSV file:")
            print(file.readline())  # Header line
            print(file.readline())  # First data line
            file.seek(0)  # Reset file pointer to beginning
            
            csv_reader = csv.DictReader(file)
            records_inserted = 0
            
            for row in csv_reader:
                try:
                    cursor.execute('''
                        INSERT INTO diabetes_data (
                            gender, age, hypertension, heart_disease, 
                            smoking_history, bmi, HbA1c_level, 
                            blood_glucose_level, diabetes
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (
                        row['gender'],
                        int(float(row['age'])),
                        int(row['hypertension']),
                        int(row['heart_disease']),
                        row['smoking_history'],
                        float(row['bmi']),
                        float(row['HbA1c_level']),
                        int(float(row['blood_glucose_level'])),
                        int(row['diabetes'])
                    ))
                    records_inserted += 1
                except KeyError as e:
                    print(f"\nError: Missing column in CSV file: {e}")
                    print("Expected columns: gender, age, hypertension, heart_disease, smoking_history, bmi, HbA1c_level, blood_glucose_level, diabetes")
                    print("Found columns:", list(row.keys()))
                    return False
                except ValueError as e:
                    print(f"\nError converting value in row {records_inserted + 1}: {e}")
                    print("Row data:", row)
                    return False
                
        # Commit changes
        conn.commit()
        print(f"\nSuccessfully inserted {records_inserted} records into the database.")
        return True
        
    except Error as e:
        print(f"\nDatabase error: {e}")
        return False
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def query_database():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("\nDiabetes Dataset Analysis:")
        
        # Check if we have any data
        cursor.execute('SELECT COUNT(*) FROM diabetes_data')
        count = cursor.fetchone()[0]
        
        if count == 0:
            print("No data found in the database.")
            return
            
        # Diabetes prevalence by age group
        print("\n1. Diabetes prevalence by age group:")
        cursor.execute('''
            SELECT 
                CASE 
                    WHEN age < 20 THEN '0-19'
                    WHEN age < 40 THEN '20-39'
                    WHEN age < 60 THEN '40-59'
                    ELSE '60+'
                END as age_group,
                COUNT(*) as total_count,
                SUM(diabetes) as diabetes_count,
                ROUND(SUM(diabetes) / COUNT(*) * 100, 2) as diabetes_percentage
            FROM diabetes_data
            GROUP BY age_group
            ORDER BY age_group
        ''')
        for row in cursor.fetchall():
            print(f"Age group {row[0]}: {row[1]} people, {row[2]} with diabetes ({row[3]}%)")
        
        # Average BMI and HbA1c statistics by diabetes status
        print("\n2. Health metrics by diabetes status:")
        cursor.execute('''
            SELECT 
                diabetes,
                COUNT(*) as count,
                ROUND(AVG(bmi), 2) as avg_bmi,
                ROUND(AVG(HbA1c_level), 2) as avg_HbA1c,
                ROUND(AVG(blood_glucose_level), 2) as avg_glucose
            FROM diabetes_data
            GROUP BY diabetes
        ''')
        for row in cursor.fetchall():
            status = "With diabetes" if row[0] == 1 else "Without diabetes"
            print(f"\n{status}:")
            print(f"Count: {row[1]}")
            print(f"Average BMI: {row[2]}")
            print(f"Average HbA1c: {row[3]}")
            print(f"Average Blood Glucose: {row[4]}")
        
    except Error as e:
        print(f"Database error: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    print("Current directory:", os.getcwd())
    csv_file = input("Enter the CSV file name (e.g., diabetes.csv): ")
    
    if create_diabetes_database(csv_file):
        query_database()