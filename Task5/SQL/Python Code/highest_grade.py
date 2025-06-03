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

# SQL Query
query = """
SELECT 
    sc.school_name,
    st.first_name || ' ' || st.last_name AS full_name,
    st.grade,
    RANK() OVER (
        PARTITION BY st.school_id
        ORDER BY st.grade DESC
    ) AS grade_rank
FROM 
    student st
JOIN 
    school sc ON st.school_id = sc.school_id
ORDER BY 
    sc.school_name, grade_rank;
"""

# Execute and fetch
cur.execute(query)
results = cur.fetchall()

# Format with pandas
df = pd.DataFrame(results, columns=["School Name", "Student Name", "Grade", "Grade Rank"])
print(df)

cur.close()
conn.close()