INSERT DELAYED IGNORE INTO tracks (id,release_id, title)
VALUES (%s, %s, %s);