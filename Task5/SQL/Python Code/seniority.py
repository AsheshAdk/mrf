import psycopg2
import pandas as pd
from datetime import date

# To connect postsql
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="Ashesh@123",
    host="localhost",
    port="5432"
)

cur = conn.cursor()

# Fetch student names and enrollment dates
query = """
SELECT 
    first_name || ' ' || last_name AS student_name,
    enrollment_date
FROM student;
"""
cur.execute(query)
students = cur.fetchall()

# calculate seniority
def calculate_seniority(enroll_date):
    today = date.today()
    years = today.year - enroll_date.year - ((today.month, today.day) < (enroll_date.month, enroll_date.day))
    return f"{years} years"

# result list
data = []
for name, enroll_date in students:
    seniority = calculate_seniority(enroll_date)
    data.append((name, enroll_date, seniority))

# Display with pandas
df = pd.DataFrame(data, columns=["Student Name", "Enrollment Date", "Seniority"])
print(df)

cur.close()
conn.close()
