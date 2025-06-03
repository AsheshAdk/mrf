import psycopg2
import pandas as pd

# PostgreSQL connection
conn = psycopg2.connect(
    dbname="postgres",    
    user="postgres",      
    password="Ashesh@123",  
    host="localhost",          
    port="5432"                
)

# Create cursor
cur = conn.cursor()

# SQL query
query = """
SELECT 
    s.school_name,
    CONCAT(t.first_name, ' ', t.last_name) AS teacher_name,
    t.hire_date,
    t.salary,
    SUM(t.salary) OVER (
        PARTITION BY s.school_id
    ) AS cumulative_salary
FROM 
    teacher t
JOIN 
    school s ON t.school_id = s.school_id
ORDER BY 
    t.hire_date;
"""

# Execute query
cur.execute(query)

# Fetch and format result
rows = cur.fetchall()
columns = ["School Name", "Teacher Name", "Hire Date", "Salary", "Cumulative Salary"]
df = pd.DataFrame(rows, columns=columns)

print(df)

cur.close()
conn.close()