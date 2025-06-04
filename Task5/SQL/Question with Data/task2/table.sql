-- Create series table
CREATE TABLE public.series (
    series_id SERIAL PRIMARY KEY,
    series_name VARCHAR(100) NOT NULL,
    genre VARCHAR(50) NOT NULL CHECK (genre IN ('Drama', 'Comedy', 'Sci-Fi', 'Mystery', 'Fantasy', 'Action')),
    release_year INTEGER NOT NULL CHECK (release_year >= 1990)
);

-- Create episode table
CREATE TABLE public.episode (
    episode_id SERIAL PRIMARY KEY,
    series_id INTEGER NOT NULL REFERENCES public.series(series_id),
    season_number INTEGER NOT NULL CHECK (season_number >= 1),
    episode_number INTEGER NOT NULL CHECK (episode_number >= 1),
    episode_title VARCHAR(100) NOT NULL,
    air_date DATE NOT NULL,
    rating NUMERIC(3, 1) CHECK (rating >= 0 AND rating <= 10)
);

-- Insert data into series table
INSERT INTO public.series (series_name, genre, release_year) VALUES
('Game of Thrones', 'Fantasy', 2011),
('Friends', 'Comedy', 1994),
('Breaking Bad', 'Drama', 2008),
('Spartacus', 'Action', 2010),
('Lost', 'Mystery', 2004),
('Stranger Things', 'Sci-Fi', 2016),
('The Boys', 'Action', 2019);

-- Insert data into episode table
INSERT INTO public.episode (series_id, season_number, episode_number, episode_title, air_date, rating) VALUES
(1, 1, 1, 'Winter Is Coming', '2011-04-17', 9.0),
(1, 1, 2, 'The Kingsroad', '2011-04-24', 8.8),
(1, 2, 1, 'The North Remembers', '2012-04-01', 8.9),
(2, 1, 1, 'The One Where Monica Gets a Roommate', '1994-09-22', 8.3),
(2, 1, 2, 'The One with the Sonogram', '1994-09-29', 8.1),
(2, 2, 1, 'The One with Ross''s New Girlfriend', '1995-09-21', 8.4),
(3, 1, 1, 'Pilot', '2008-01-20', 8.7),
(3, 2, 1, 'Seven Thirty-Seven', '2009-03-08', 8.6),
(3, 3, 1, 'No Más', '2010-03-21', 8.5),
(4, 1, 1, 'The Red Woman', '2010-01-22', 8.2),
(4, 1, 2, 'Whispers', '2010-01-29', 8.0),
(4, 2, 1, 'Fugitivus', '2012-01-27', 8.3),
(5, 1, 1, 'Pilot', '2004-09-22', 8.9),
(5, 1, 2, 'Tabula Rasa', '2004-09-29', 8.5),
(5, 2, 1, 'Man of Science, Man of Faith', '2005-09-21', 8.7),
(6, 1, 1, 'Chapter One: The Vanishing', '2016-07-15', 8.5),
(6, 1, 2, 'Chapter Two: The Weirdo', '2016-07-15', 8.2),
(6, 2, 1, 'Chapter Nine: The Gate', '2017-10-27', 8.8),
(7, 1, 1, 'The Name of the Game', '2019-07-26', 8.6),
(7, 1, 2, 'Cherry', '2019-07-26', 8.4),
(7, 2, 1, 'The Self-Preservation Society', '2020-09-04', 8.7);

select * from series
select * from episode