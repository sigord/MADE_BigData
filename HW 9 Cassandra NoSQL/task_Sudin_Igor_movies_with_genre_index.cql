SELECT COUNT(*) FROM movies_with_genre_index WHERE year >= 1980 AND year <= 1990 AND genres CONTAINS 'Horror' ALLOW FILTERING;
