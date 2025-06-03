import psycopg2

# Database connection
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
    s.school_name,
    COUNT(st.student_id) AS student_count
FROM 
    school s
LEFT JOIN 
    student st ON s.school_id = st.school_id
GROUP BY 
    s.school_name
ORDER BY 
    student_count DESC;
"""

# Execute and fetch results
cur.execute(query)
results = cur.fetchall()

for row in results:
    print(f"School: {row[0]}, Students: {row[1]}")
cur.close()
conn.close()
