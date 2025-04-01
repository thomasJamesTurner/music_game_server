INSERT DELAYED IGNORE INTO albums
    (id,artist, title, country, genre, style, release_date, discogs_url)  
VALUES 
    (%s, %s, %s, %s, %s, %s, %s, %s);