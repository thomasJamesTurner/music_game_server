INSERT INTO releases (artist, title, country, label, catalog_number, format, speed, genre, style, release_date, description, discogs_url)
VALUES %s
RETURNING id;