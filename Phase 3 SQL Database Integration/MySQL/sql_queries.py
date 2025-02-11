import mysql.connector
from mysql.connector import Error

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',  
    'password': 'root@123',  
    'database': 'diabetes_db'
}

def execute_query(cursor, query, description):
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        
        print(f"\n{description}")
        print("-" * 50)
        
        columns = [desc[0] for desc in cursor.description]
        print(" | ".join(columns))
        print("-" * 50)
        
        for row in results:
            print(" | ".join(str(value) for value in row))
        
    except Error as e:
        print(f"Error executing query: {e}")

def analyze_diabetes_data():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("1. Total number of patients")
        count_query = "SELECT COUNT(*) as total_patients FROM diabetes_data"
        execute_query(cursor, count_query, "Total number of patients")
        
        print("2. Number of diabetic and non-diabetic patients")
        diabetes_query = """
        SELECT 
            diabetes as is_diabetic,
            COUNT(*) as patient_count
        FROM diabetes_data
        GROUP BY diabetes
        """
        execute_query(cursor, diabetes_query, "Count of diabetic (1) vs non-diabetic (0) patients")
        
        print(" 3. Gender distribution")
        gender_query = """
        SELECT 
            gender,
            COUNT(*) as count
        FROM diabetes_data
        GROUP BY gender
        """
        execute_query(cursor, gender_query, "Number of patients by gender")
        
        print("# 4. Average age and BMI")
        avg_query = """
        SELECT 
            ROUND(AVG(age), 0) as avg_age,
            ROUND(AVG(bmi), 1) as avg_bmi
        FROM diabetes_data
        """
        execute_query(cursor, avg_query, "Average age and BMI of all patients")
        
        print("5. Smoking history counts")
        smoking_query = """
        SELECT 
            smoking_history,
            COUNT(*) as count
        FROM diabetes_data
        GROUP BY smoking_history
        ORDER BY count DESC
        """
        execute_query(cursor, smoking_query, "Smoking history distribution")
        
        print("6. Patients by age group")
        age_query = """
        SELECT 
            CASE 
                WHEN age < 40 THEN 'Under 40'
                WHEN age < 60 THEN '40-59'
                ELSE '60 and above'
            END as age_group,
            COUNT(*) as count
        FROM diabetes_data
        GROUP BY age_group
        ORDER BY age_group
        """
        execute_query(cursor, age_query, "Patients by age group")

    except Error as e:
        print(f"Database error: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    analyze_diabetes_data()