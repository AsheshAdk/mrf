import psycopg2
import pandas as pd

# Connect through PostgreSQL
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="Ashesh@123",
    host="localhost",
    port="5432"
)

cur = conn.cursor()

query = """
WITH teacher_student_count AS (
    SELECT 
        t.teacher_id,
        t.school_id,
        COUNT(s.student_id) AS student_count
    FROM 
        teacher t
    JOIN 
        student s ON s.teacher_id = t.teacher_id
    GROUP BY 
        t.teacher_id, t.school_id
),
ranked_teachers AS (
    SELECT 
        teacher_id,
        school_id,
        student_count,
        RANK() OVER (
            PARTITION BY school_id
            ORDER BY student_count DESC
        ) AS teacher_rank
    FROM teacher_student_count
)
SELECT 
    sch.school_name,
    CONCAT(t.first_name, ' ', t.last_name) AS teacher_name,
    CONCAT(s.first_name, ' ', s.last_name) AS student_name,
    tsc.student_count,
    rt.teacher_rank
FROM 
    student s
JOIN 
    teacher t ON s.teacher_id = t.teacher_id
JOIN 
    school sch ON s.school_id = sch.school_id
JOIN 
    teacher_student_count tsc ON t.teacher_id = tsc.teacher_id
JOIN 
    ranked_teachers rt ON t.teacher_id = rt.teacher_id
ORDER BY 
    sch.school_name, rt.teacher_rank, teacher_name, student_name;
"""

cur.execute(query)
rows = cur.fetchall()
columns = ["School Name", "Teacher Name", "Student Name", "Student Count", "Teacher Rank"]

df = pd.DataFrame(rows, columns=columns)
print(df)

cur.close()
conn.close()