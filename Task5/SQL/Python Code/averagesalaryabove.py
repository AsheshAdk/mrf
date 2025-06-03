import psycopg2
import pandas as pd

# connection of postsql
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="Ashesh@123",
    host="localhost",
    port="5432"
)

cur = conn.cursor()

# SQL query
query = """
SELECT 
    department,
    ROUND(AVG(salary), 2) AS avg_salary
FROM 
    teacher
GROUP BY 
    department
HAVING 
    AVG(salary) > 55000
ORDER BY 
    avg_salary DESC;
"""

# Execute and fetch
cur.execute(query)
results = cur.fetchall()

# Format with pandas
df = pd.DataFrame(results, columns=["Department", "Average Salary"])
print(df)

cur.close()
conn.close()