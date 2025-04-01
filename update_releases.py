from lxml import etree
import database_funcs

def parse_and_insert(xml_file,insert_file, batch_size=1000,tag="release"):

    db = database_funcs.db_connect()
    cursor = db.cursor()

    context = etree.iterparse(xml_file, events=("end",), tag=tag)  # Change 'record' to match your XML
    batch = []

    for _, elem in context:
        data = None
        if tag == "release":
            data = extract_release_data(elem)
        elif tag == "track":
            data = extract_track_data(elem)
        batch.append(data)

        if len(batch) >= batch_size:  # Insert in batches
            insert_batch(cursor, batch,filename=insert_file)
            db.commit()
            batch.clear()  # Clear memory

        elem.clear()  # Free memory
        while elem.getprevious() is not None:
            del elem.getparent()[0]  # Remove previous elements

    if batch:  # Insert remaining records
        insert_batch(cursor, batch,filename=insert_file)

    
    cursor.close()
    db.close()

def preview_xml(file_path, num_elements=5):

    context = etree.iterparse(file_path, events=("end",))
    
    count = 0
    for event, elem in context:
        print(etree.tostring(elem, pretty_print=True).decode())  # Print XML snippet
        count += 1
        if count >= num_elements:
            break  # Stop after printing `num_elements`
    
    print("\n(Preview complete. Only first few elements shown.)")

def find_release_id(track_elem):
    """Find release ID by traversing up the tree from a track element."""
    parent = track_elem.getparent()
    while parent is not None:
        release_id = parent.findtext(".//id")
        if release_id:
            return release_id
        parent = parent.getparent()  # Keep moving up the tree
    return None  # Return None if no release ID is found

def clean_date(date):
    if date.find("-") == -1:
        return (date + "-01-01")
    if date.find("-00") != -1:
        return date.replace("-00","-01")
    
    return date

def extract_release_data(elem):
    
    
    artist = elem.findtext(".//artist/name", default="")
    title = elem.findtext(".//title", default="")
    country = elem.findtext(".//country", default="")
    #label = elem.findtext(".//label[@name]", default="")
    #catalog_number = elem.findtext(".//label[@catno]", default="")
    #format_name = elem.findtext(".//format[@name]", default="")
    #speed = next((d.text for d in elem.findall(".//description") if "RPM" in d.text), None)
    genre = elem.findtext(".//genre", default="")
    style = elem.findtext(".//style", default="")
    release_date = elem.findtext(".//released", default="")
    #description = elem.findtext(".//notes", default="")
    discogs_url = next((d.text.split("\n")[0] for d in elem.findall(".//description") if "discogs.com" in d.text), "")

    id = database_funcs.getID(title+artist)
    return (id, artist, title, country, genre, style, clean_date(release_date), discogs_url)

def extract_track_data(elem):
    title = elem.findtext(".//title", default="")    

    return (database_funcs.getID(title),find_release_id(elem),title)





def insert_to_db(db,insert_file,data):
    cursor = db.cursor()
    query = open("sql/"+insert_file,"r").readlines()
    cursor.execute(query,data)

def insert_batch(cursor, batch,filename):

    query = database_funcs.load_query(filename)
    
    cursor.executemany(query, batch)
    print(f"Rows inserted: {cursor.rowcount}")


# Example usage
#preview_xml("C:\\Users\\rokkk\\Downloads\\discogs_20250101_releases.xml\\discogs_20250101_releases.xml",85)
parse_and_insert("C:\\Users\\rokkk\\Downloads\\discogs_20250101_releases.xml\\discogs_20250101_releases.xml",
                 tag="track",
                 insert_file="insert_into_track.sql",
                 batch_size= 100000)
