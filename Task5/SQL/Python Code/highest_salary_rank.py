import psycopg2
import pandas as pd

# connection to sql
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="Ashesh@123",
    host="localhost",
    port="5432"
)

# Cursor executing queries
cur = conn.cursor()

# rank teachers by salary within each school
query = """
SELECT 
    t.first_name || ' ' || t.last_name AS teacher_name,
    s.school_name,
    t.salary,
    RANK() OVER (
        PARTITION BY t.school_id
        ORDER BY t.salary DESC
    ) AS salary_rank
FROM 
    teacher t
JOIN 
    school s ON t.school_id = s.school_id
ORDER BY 
    s.school_name, salary_rank;
"""

# Execute query and fetch results
cur.execute(query)
results = cur.fetchall()

# DataFrame for better readability wich was converted
df = pd.DataFrame(results, columns=["Teacher Name", "School Name", "Salary", "Salary Rank"])
print(df)

cur.close()
conn.close()
