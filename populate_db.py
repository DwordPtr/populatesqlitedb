import csv, sqlite3
import logging
from csv import reader

def dedup(list_of_rows):
    seen_ids = set()
    uniq_things = []
    for tpl in list_of_rows:
        (_, thing_id) = tpl
        if thing_id not in seen_ids:
            uniq_things.append(tpl)

        seen_ids.add(thing_id)
    return uniq_things

#for some reason there's a duplicate scrobble
def dedup_scrobbles(to_scrobble):
    seen_keys = set()
    uniq_rows = []
    for tpl in to_scrobble:
        (uts, utc_time, artist_mbid, album_mbid, track_mbid) = tpl
        pk = (utc_time, track_mbid)
        # we'll lose data but get sqlite to stop complaining
        if pk not in seen_keys and utc_time and track_mbid and artist_mbid and album_mbid and uts:
            uniq_rows.append(tpl)

        seen_keys.add(pk)
    return uniq_rows


db_file = 'scrobbles.db'
file_name = 'scrobbles-exneo002-1712949461.csv'
keys = [ 'uts','utc_time','artist','artist_mbid','album','album_mbid','track','track_mbid']
con = sqlite3.connect(db_file) # change to 'sqlite:///scrobbles.db'
cur = con.cursor()
cur.execute("CREATE TABLE artist (artist not null, artist_mbid not null primary key);") # use your column names here
cur.execute("CREATE TABLE album (album not null, album_mbid not null primary key);") # use your column names here
cur.execute("CREATE TABLE track (track not null, track_mbid not null primary key);") # use your column names here
#it's a pretty reasonable assumption a human can't listen to the same song twice
#also this is an ugly table creation
cur.execute("""CREATE TABLE scrobble 
(uts, utc_time, artist_mbid, album_mbid, track_mbid, PRIMARY KEY(track_mbid, utc_time),
FOREIGN KEY (artist_mbid) REFERENCES artist(artist_mbid),
FOREIGN KEY (album_mbid) REFERENCES album(album_mbid),
FOREIGN KEY (track_mbid) REFERENCES track(track_mbid));
""") # use your column names here
#
with open(file_name,'r') as fin: # `with` statement available in 2.5+
    # csv.DictReader uses first line in file for column headings by default
    dr = csv.DictReader(fin) # comma is default delimiter
    to_artist = [(i['artist'], i['artist_mbid']) for i in dr]
    to_artist = dedup(to_artist)
    print(f'artist to add {len(to_artist)}')

with open(file_name, 'r') as fin:
    # csv.DictReader uses first line in file for column headings by default
    dr = csv.DictReader(fin) # comma is default delimiter
    to_album = [(i['album'], i['album_mbid']) for i in dr]
    to_album = dedup(to_album)
    print(f'album to add {len(to_album)}')

with open(file_name, 'r') as fin:
    # csv.DictReader uses first line in file for column headings by default
    dr = csv.DictReader(fin) # comma is default delimiter
    to_track = [(i['track'], i['track_mbid']) for i in dr]
    to_track = dedup(to_track)
    print(f'track to add {len(to_track)}')
    con.commit()
    con.close()

with open(file_name, 'r') as fin:
    con = sqlite3.connect(db_file) # change to 'sqlite:///scrobbles.db'
    cur = con.cursor()
    # csv.DictReader uses first line in file for column headings by default
    dr = csv.DictReader(fin) # comma is default delimiter
    #every scrobble should be uniq so it doesn't need deduplication
    to_scrobble = [(str(i['uts']), str(i['utc_time']), str(i['artist_mbid']), str(i['album_mbid']), str(i['track_mbid'])) for i in dr]
    to_scrobble = dedup_scrobbles(to_scrobble)
    print(f'scrobble to add {len(to_scrobble)}')

cur.executemany("INSERT INTO artist (artist, artist_mbid) VALUES (?, ?);", to_artist)
cur.executemany("INSERT INTO album (album, album_mbid) VALUES (?, ?);", to_album)
cur.executemany("INSERT INTO track (track, track_mbid) VALUES (?, ?);", to_track)
cur.executemany("INSERT INTO scrobble (uts, utc_time, artist_mbid, album_mbid, track_mbid) VALUES (?, ?, ?, ?, ?);", to_scrobble)
con.commit()
con.close()
