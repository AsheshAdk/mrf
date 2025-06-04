import psycopg2
import pandas as pd
from datetime import date

# Database Connection
DB_CONFIG = {
    'host': 'localhost',     
    'port': 5432,
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'Ashesh@123'
}

# SQL Script
SQL_SCRIPT = """
-- Create the longevity score function
CREATE OR REPLACE FUNCTION public.calculate_longevity_score(rating NUMERIC, season_number INT)
RETURNS NUMERIC AS $$
DECLARE
    score NUMERIC;
BEGIN
    score := COALESCE(rating, 0) * 3 + season_number * 2;
    RETURN ROUND(score, 2);
END;
$$ LANGUAGE plpgsql;

-- Final Analysis Query
WITH longevity_scores AS (
    SELECT
        s.series_id,
        s.series_name,
        s.genre,
        e.episode_title,
        e.rating,
        calculate_longevity_score(e.rating, e.season_number) AS longevity_score,
        CASE
            WHEN calculate_longevity_score(e.rating, e.season_number) >= 30 THEN 'Long-Lasting'
            WHEN calculate_longevity_score(e.rating, e.season_number) >= 25 THEN 'Moderate'
            ELSE 'Short-Lived'
        END AS episode_label,
        e.season_number,
        e.air_date
    FROM episode e
    JOIN series s ON e.series_id = s.series_id
),
episode_counts AS (
    SELECT
        series_id,
        COUNT(*) AS episode_count,
        MAX(season_number) AS total_seasons
    FROM episode
    GROUP BY series_id
)

SELECT
    ls.series_name,
    ls.episode_title,
    ls.rating,
    ls.longevity_score,
    ls.episode_label,
    ec.episode_count,
    COALESCE((
        SELECT STRING_AGG(DISTINCT s2.genre, ', ' ORDER BY s2.genre)
        FROM episode e2
        JOIN series s2 ON e2.series_id = s2.series_id
        WHERE e2.air_date > DATE '2015-01-01'
        AND s2.series_id = ls.series_id
    ), 'No Episodes') AS recent_genres,
    ec.total_seasons
FROM longevity_scores ls
JOIN episode_counts ec ON ls.series_id = ec.series_id
ORDER BY ls.series_name, ls.longevity_score DESC;
"""

#Python Function to Execute
def run_longevity_analysis():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()

        print("Connected to database. Executing function and query...")
        cursor.execute(SQL_SCRIPT)

        cursor.execute("""
            SELECT
                ls.series_name,
                ls.episode_title,
                ls.rating,
                ls.longevity_score,
                ls.episode_label,
                ec.episode_count,
                COALESCE((
                    SELECT STRING_AGG(DISTINCT s2.genre, ', ' ORDER BY s2.genre)
                    FROM episode e2
                    JOIN series s2 ON e2.series_id = s2.series_id
                    WHERE e2.air_date > DATE '2015-01-01'
                    AND s2.series_id = ls.series_id
                ), 'No Episodes') AS recent_genres,
                ec.total_seasons
            FROM (
                SELECT
                    s.series_id,
                    s.series_name,
                    s.genre,
                    e.episode_title,
                    e.rating,
                    calculate_longevity_score(e.rating, e.season_number) AS longevity_score,
                    CASE
                        WHEN calculate_longevity_score(e.rating, e.season_number) >= 30 THEN 'Long-Lasting'
                        WHEN calculate_longevity_score(e.rating, e.season_number) >= 25 THEN 'Moderate'
                        ELSE 'Short-Lived'
                    END AS episode_label,
                    e.season_number,
                    e.air_date
                FROM episode e
                JOIN series s ON e.series_id = s.series_id
            ) ls
            JOIN (
                SELECT
                    series_id,
                    COUNT(*) AS episode_count,
                    MAX(season_number) AS total_seasons
                FROM episode
                GROUP BY series_id
            ) ec ON ls.series_id = ec.series_id
            ORDER BY ls.series_name, ls.longevity_score DESC;
        """)

        rows = cursor.fetchall()
        print("Fetched rows:", rows)

        df = pd.DataFrame(rows, columns=[
            'series_name', 'episode_title', 'rating', 'longevity_score', 'episode_label',
            'episode_count', 'recent_genres', 'total_seasons'
        ])

        print(df)
        df.to_csv("longevity_report.csv", index=False)
        print("Saved report to longevity_report.csv")

        cursor.close()
        conn.close()
        return df

    except Exception as e:
        print("Error:", e)
run_longevity_analysis()
