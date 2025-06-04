import psycopg2
import pandas as pd
from datetime import datetime

# Database connection
conn = psycopg2.connect(
    dbname='postgres',  
    user='postgres',
    password='Ashesh@123',
    host='localhost',
    port='5432'
)

cur = conn.cursor()

# Create score function
create_function_sql = """
CREATE OR REPLACE FUNCTION calculate_impact_score(rating NUMERIC, air_date DATE)
RETURNS NUMERIC AS $$
DECLARE
    months_since_air INT;
    score NUMERIC;
BEGIN
    months_since_air := DATE_PART('year', CURRENT_DATE) * 12 + DATE_PART('month', CURRENT_DATE)
                        - (DATE_PART('year', air_date) * 12 + DATE_PART('month', air_date));

    score := COALESCE(rating, 0) * 5 + (months_since_air / 12.0);
    RETURN ROUND(score, 2);
END;
$$ LANGUAGE plpgsql;
"""
cur.execute(create_function_sql)
conn.commit()

# query
analysis_query = """
WITH episode_scores AS (
    SELECT
        e.episode_id,
        s.series_name,
        e.episode_title,
        calculate_impact_score(e.rating, e.air_date) AS impact_score,
        CASE
            WHEN calculate_impact_score(e.rating, e.air_date) >= 45 THEN 'High Impact'
            WHEN calculate_impact_score(e.rating, e.air_date) >= 40 THEN 'Moderate'
            ELSE 'Low'
        END AS episode_category,
        e.rating,
        s.series_id
    FROM episode e
    JOIN series s ON e.series_id = s.series_id
),
avg_ratings AS (
    SELECT series_id, ROUND(AVG(rating), 2) AS avg_series_rating
    FROM episode
    GROUP BY series_id
),
ranked_episodes AS (
    SELECT *,
           RANK() OVER (PARTITION BY series_id ORDER BY impact_score DESC) AS rank
    FROM episode_scores
),
high_rated AS (
    SELECT
        series_id,
        STRING_AGG(episode_title, ', ' ORDER BY impact_score DESC) AS high_rated_episodes
    FROM ranked_episodes
    WHERE episode_category = 'High Impact'
    GROUP BY series_id
)

SELECT
    r.series_name,
    r.episode_title,
    r.impact_score,
    r.episode_category,
    r.rank,
    h.high_rated_episodes,
    a.avg_series_rating
FROM ranked_episodes r
LEFT JOIN high_rated h ON r.series_id = h.series_id
LEFT JOIN avg_ratings a ON r.series_id = a.series_id
WHERE r.rank <= 3
ORDER BY r.series_name, r.rank;
"""

# Load results into a DataFrame
cur.execute(analysis_query)
columns = [desc[0] for desc in cur.description]
rows = cur.fetchall()
df = pd.DataFrame(rows, columns=columns)

print(df)

cur.close()
conn.close()