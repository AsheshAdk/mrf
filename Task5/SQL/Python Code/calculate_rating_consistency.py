import psycopg2
import pandas as pd
from datetime import date

# Configuration 
DB_CONFIG = {
    'host': 'localhost',       
    'port': 5432,
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'Ashesh@123'
}

#SQL
SQL_SCRIPT = """
-- Create the rating consistency score function
CREATE OR REPLACE FUNCTION public.calculate_rating_consistency(rating NUMERIC, series_avg_rating NUMERIC)
RETURNS NUMERIC AS $$
DECLARE
    score NUMERIC;
BEGIN
    score := ABS(COALESCE(rating, 0) - COALESCE(series_avg_rating, 0)) * 10;
    RETURN ROUND(score, 2);
END;
$$ LANGUAGE plpgsql;

-- Final Analysis Query
WITH series_avg AS (
    SELECT
        series_id,
        MAX(rating) AS max_series_rating,
        AVG(rating) AS avg_series_rating
    FROM episode
    GROUP BY series_id
),
consistency_scores AS (
    SELECT
        s.series_name,
        e.episode_title,
        e.rating,
        sa.avg_series_rating,
        calculate_rating_consistency(e.rating, sa.avg_series_rating) AS consistency_score,
        CASE
            WHEN calculate_rating_consistency(e.rating, sa.avg_series_rating) <= 5 THEN 'Consistent'
            WHEN calculate_rating_consistency(e.rating, sa.avg_series_rating) <= 15 THEN 'Variable'
            ELSE 'Highly Variable'
        END AS consistency_category,
        e.season_number,
        e.air_date,
        e.series_id
    FROM episode e
    JOIN series s ON e.series_id = s.series_id
    JOIN series_avg sa ON e.series_id = sa.series_id
),
ranked AS (
    SELECT *,
        RANK() OVER (PARTITION BY series_id, season_number ORDER BY rating DESC) AS season_rank
    FROM consistency_scores
)
SELECT
    r.series_name,
    r.episode_title,
    r.consistency_score,
    r.consistency_category,
    r.season_rank,
    COALESCE((
        SELECT STRING_AGG(e2.episode_title, ', ' ORDER BY e2.episode_title)
        FROM episode e2
        WHERE e2.series_id = r.series_id AND e2.air_date > DATE '2015-06-01'
    ), 'None') AS recent_episodes,
    sa.max_series_rating
FROM ranked r
JOIN series_avg sa ON r.series_id = sa.series_id
ORDER BY r.series_name, r.consistency_score;
"""

#Python Function to Execute
def run_consistency_analysis():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()

        print("Connected to database. Executing function and query...")
        cursor.execute(SQL_SCRIPT)

        cursor.execute("""
            SELECT
                r.series_name,
                r.episode_title,
                r.consistency_score,
                r.consistency_category,
                r.season_rank,
                COALESCE((
                    SELECT STRING_AGG(e2.episode_title, ', ' ORDER BY e2.episode_title)
                    FROM episode e2
                    WHERE e2.series_id = r.series_id AND e2.air_date > DATE '2015-06-01'
                ), 'None') AS recent_episodes,
                sa.max_series_rating
            FROM (
                SELECT
                    s.series_name,
                    e.episode_title,
                    e.rating,
                    sa.avg_series_rating,
                    calculate_rating_consistency(e.rating, sa.avg_series_rating) AS consistency_score,
                    CASE
                        WHEN calculate_rating_consistency(e.rating, sa.avg_series_rating) <= 5 THEN 'Consistent'
                        WHEN calculate_rating_consistency(e.rating, sa.avg_series_rating) <= 15 THEN 'Variable'
                        ELSE 'Highly Variable'
                    END AS consistency_category,
                    e.season_number,
                    e.air_date,
                    e.series_id,
                    RANK() OVER (PARTITION BY e.series_id, e.season_number ORDER BY e.rating DESC) AS season_rank
                FROM episode e
                JOIN series s ON e.series_id = s.series_id
                JOIN (
                    SELECT series_id, MAX(rating) AS max_series_rating, AVG(rating) AS avg_series_rating
                    FROM episode
                    GROUP BY series_id
                ) sa ON e.series_id = sa.series_id
            ) r
            JOIN (
                SELECT series_id, MAX(rating) AS max_series_rating
                FROM episode
                GROUP BY series_id
            ) sa ON r.series_id = sa.series_id
            ORDER BY r.series_name, r.consistency_score;
        """)

        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=[
            'series_name', 'episode_title', 'consistency_score', 'consistency_category',
            'season_rank', 'recent_episodes', 'max_series_rating'
        ])

        print(df)
        df.to_csv("consistency_report.csv", index=False)
        print("Saved report to consistency_report.csv")

        cursor.close()
        conn.close()
        return df

    except Exception as e:
        print("Error:", e)

run_consistency_analysis()
