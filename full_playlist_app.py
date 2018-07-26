import pyodbc 
import requests
import sys
import datetime
import logging
import configparser

# ====================== METHOD DEFINITIONS ========================
def auth_header():
    return {'Authorization': 'Bearer {}'.format(spotify_token)}

# obtain new Spotify access token using refresh_token
def get_new_access_token():
	basic_token = config['DEFAULT']['ENCODED_BASIC_TOKEN']
	refresh_token = config['DEFAULT']['REFRESH_TOKEN']

	req_header = {'Authorization': 'Basic {}'.format(basic_token)}
	req_body = {'grant_type': 'refresh_token', 'refresh_token': refresh_token}
	r = requests.post('https://accounts.spotify.com/api/token', headers=req_header, data=req_body)
	res_json = r.json()
		
	new_token = res_json['access_token']
	# update token in the database
	cursor.execute("UPDATE tokens SET value = ? WHERE token_type = 'access_token'", (new_token))
	logging.info("Generated new access token.") 
	cnxn.commit()

	return new_token
	
# populate playlist details based on the playlist object
def populate_playlist(playlist_object):
	playlist_name = playlist_object['name']
	playlist_id = playlist_object['id']
	collaborative = playlist_object['collaborative']	
	snapshot_id = playlist_object['snapshot_id']
	description = playlist_object['description']
	href = playlist_object['href']
	pub = playlist_object['public']
	uri = playlist_object['uri']
	user_id = playlist_object['owner']['id']	
	try:        
		cursor.execute("INSERT INTO PLAYLIST (PLAYLIST_NAME, PLAYLIST_ID, COLLABORATIVE, SNAPSHOT_ID, DESCRIPTION, HREF, PUB, URI, USER_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (playlist_name, playlist_id, collaborative, snapshot_id, description, href, pub, uri, user_id))
		logging.info("Created new entry for playlist {}.".format(playlist_id)) 
		cnxn.commit()
	except pyodbc.IntegrityError:
		logging.warning("Violation of PRIMARY KEY constraint for playlist {}.".format(playlist_id))
	except: 
		logging.warning(sys.exc_info())
		logging.warning("Was not able to insert data for playlist {}.".format(playlist_id)) 
		
# populate playlist track based on the playlist_id and track object
def populate_playlist_track(playlist_track, playlist_id):
	track_id = playlist_track['track']['id']	
	is_local = playlist_track['is_local']
	added_by = playlist_track['added_by']['id']
	added_at = playlist_track['added_at']
	try:        
		cursor.execute("INSERT INTO PLAYLIST_TRACK (PLAYLIST_ID, TRACK_ID, IS_LOCAL, ADDED_BY, ADDED_AT) VALUES (?,?,?,?,?)", (playlist_id, track_id, is_local, added_by, added_at))
		logging.info("Created new entry for playlist-track {} for playlist {}.".format(playlist_track['track']['name'], playlist_id)) 
		cnxn.commit()
	except pyodbc.IntegrityError:
		logging.warning("Violation of PRIMARY KEY constraint for track {} in playlist {}.".format(playlist_track['track']['name'], playlist_id))
	except: 
		logging.warning(sys.exc_info())
		logging.warning("Was not able to insert data for track {} in playlist {}.".format(track_id, playlist_id)) 

# populate track based on the track object		
def populate_track(track_object):
	track_name = track_object['name']
	track_id = track_object['id']
	duration_ms = track_object['duration_ms']
	explicit = track_object['explicit']
	href = track_object['href']
	popularity = track_object['popularity']
	uri = track_object['uri']
	album_id = track_object['album']['id']
	artist_num = len(track_object['artists'])
	artist_id = track_object['artists'][0]['id']
	artist_id2 = None
	artist_id3 = None
	artist_id4 = None
	if artist_num > 2:
		artist_id2 = track_object['artists'][1]['id']
	if artist_num > 3:
		artist_id3 = track_object['artists'][2]['id']
	if artist_num > 4:
		artist_id4 = track_object['artists'][3]['id']
	try:        
		cursor.execute("INSERT INTO TRACK (TRACK_NAME, TRACK_ID, duration_ms, EXPLICIT, HREF, POPULARITY, URI, ALBUM_ID, ARTIST_ID, ARTIST_ID2, ARTIST_ID3, ARTIST_ID4) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (track_name, track_id, duration_ms, explicit, href, popularity, uri, album_id, artist_id, artist_id2, artist_id3, artist_id4))
		logging.info("Created new entry for track {} by {}.".format(track_name, track_object['artists'][0]['name'])) 
		cnxn.commit()
	except pyodbc.IntegrityError:
		logging.warning("Violation of PRIMARY KEY constraint for track {} by {}.".format(track_name, track_object['artists'][0]['name']))
	except: 
		logging.warning(sys.exc_info())
		logging.warning("Was not able to insert data for track {} by {}.".format(track_id, track_object['artists'][0]['id'])) 	

# populate artist based on the artist object			
def populate_artist(artist_object):		
	artist_name = artist_object['name']
	artist_id = artist_object['id']
	href = artist_object['href']
	popularity = artist_object['popularity']
	uri= artist_object['uri']
	try:        
		cursor.execute("INSERT INTO ARTIST (ARTIST_NAME, ARTIST_ID, HREF, POPULARITY, URI) VALUES (?, ?, ?, ?, ?)", (artist_name, artist_id, href, popularity, uri))
		logging.info("Created new entry for atist {}.".format(artist_name))  
		cnxn.commit()
	except pyodbc.IntegrityError:
		logging.warning("Violation of PRIMARY KEY constraint for artist {}.".format(artist_name))
	except: 
		logging.warning(sys.exc_info())
		logging.warning("Was not able to insert data for artist {}.".format(artist_id)) 		

# populate genres based on the artist object			
def populate_genre(artist_object):
	artist_id = artist_object['id']
	for genre in artist_object['genres']:
		genre_id = genre.upper().replace(" ", "")
		genre_name = genre
		try: 
			cursor.execute("SELECT count(*) FROM GENRE WHERE GENRE_ID = ?", (genre_id))
			genre_exists = cursor.fetchone()[0]

			if genre_exists == 0:
				cursor.execute("INSERT INTO GENRE (GENRE_ID, GENRE_NAME) VALUES (?, ?)", (genre_id, genre_name))
				logging.info("Created new entry for genre {}.".format(genre_name)) 
			cursor.execute("SELECT count(*) FROM ARTIST_GENRE WHERE GENRE_ID = ? and ARTIST_ID = ?", (genre_id, artist_id))
			genreArtistsExists = cursor.fetchone()[0]
			if genreArtistsExists == 0:
				cursor.execute("INSERT INTO ARTIST_GENRE (GENRE_ID, ARTIST_ID) VALUES (?, ?)", (genre_id, artist_id))
				logging.info("Created new entry for artist {} genre {}.".format(artist_object['name'], genre_name)) 
			cnxn.commit()
		except pyodbc.IntegrityError:
			logging.warning("Violation of PRIMARY KEY constraint for artist {} and genre {}.".format(artist_object['name'],genre_name))
		except: 
			logging.warning(sys.exc_info())
			logging.warning("Was not able to insert data for artist {} and genre.".format(artist_object['id'],)) 		

# send requests and populate genres based on the artist name from LastFM
def popolate_genre_lastfm(artist_id,artist_name):
	try:
		request = '/2.0/?method=artist.getinfo&artist={}&api_key={}&format=json'.format(artist_name,api_key)
		r = requests.get(api_url + request)
		lfreq = r.json()

		for x in range(0,len(lfreq['artist']['tags']['tag'])):
			genre = lfreq['artist']['tags']['tag'][x]['name']
			# populate 
			populate_genre_table(artist_id,artist_name,genre)
	except:		
		logging.warning(sys.exc_info())	
		logging.warning(lfreq['error'])			

# populate genres based on the artist_id and genre from LastFM	
def populate_genre_table(artist_id, artist_name, genre):
	genre_id = genre.upper().replace(" ", "")
	genre_name = genre
	try: 
		cursor.execute("SELECT count(*) FROM GENRE WHERE GENRE_ID = ?", (genre_id))
		genreExists = cursor.fetchone()[0]

		if genreExists != 0:
			cursor.execute("SELECT count(*) FROM ARTIST_GENRE WHERE GENRE_ID = ? and ARTIST_ID = ?", (genre_id, artist_id))
			genreArtistsExists = cursor.fetchone()[0]
			if genreArtistsExists == 0:
				cursor.execute("INSERT INTO ARTIST_GENRE (GENRE_ID, ARTIST_ID) VALUES (?, ?)", (genre_id, artist_id))
				logging.info("Created new entry for artist {} genre {}.".format(artist_name, genre_name)) 
		cnxn.commit()
	except pyodbc.IntegrityError:
		logging.warning("Violation of PRIMARY KEY constraint for artist {} and genre {}.".format(artist_name,genre_name))
	except: 
		logging.warning(sys.exc_info())
		logging.warning("Was not able to insert data for artist {} and genre {}.".format(artist_name,genre_name)) 		
		
# create a new playlist and populate it
def create_playlist(song_ids):
	# playlist details as specified in the config file
	playlistName = config['DEFAULT']['NEW_PLAYLIST_NAME']
	playlistDescription = config['DEFAULT']['NEW_PLAYLIST_DESCRIPTION']
	
	req_header = {'Authorization': 'Bearer {}'.format(spotify_token), 'Content-Type': 'application/json'}
	req_body = {'name': playlistName, 'description': playlistDescription}
	r = requests.post('https://api.spotify.com/v1/users/{}/playlists'.format(config['DEFAULT']['MY_SPOTIFY_USER']), headers=req_header, json=req_body)
	playlist_object = r.json()
	newplaylist_id = r.json()['id']	

	if r.status_code in [200, 201]:
		# create record in the database for new playlist
		populate_playlist(playlist_object)
		logging.info("Successfully created new playlist {}.".format(r.json()['id'])) 		
		cnxn.commit()
	else:
		logging.warning("Failed to create new playlist.")
	# add tracks to playlist
	add_tracks_to_playlist(newplaylist_id, song_ids)			

# add tracks into Spotify playlist
def add_tracks_to_playlist(playlist_id, song_ids_to_add):
    # send request to add tracks to Spotify playlist
	req_header = {'Authorization': 'Bearer {}'.format(spotify_token), 'Content-Type': 'application/json'}
	req_body = {'uris': list(map((lambda song_id: 'spotify:track:' + song_id), song_ids_to_add))}

	r = requests.post('https://api.spotify.com/v1/users/{}/playlists/{}/tracks'.format(config['DEFAULT']['MY_SPOTIFY_USER'], playlist_id), headers=req_header, json=req_body)

	if r.status_code in [200, 201]:
		logging.info("Successfully added songs {} to playlist {}.".format(song_ids_to_add, playlist_id)) 		
	else:
		logging.warning("Failed to add songs {} to playlist {}.".format(song_ids_to_add, playlist_id))

# delete tracks from Spotify playlist		
def delete_tracks_from_playlist(playlist_id, song_ids_to_add):
    # send request to add tracks to Spotify playlist
	req_header = {'Authorization': 'Bearer {}'.format(spotify_token), 'Content-Type': 'application/json'}
	req_body = {'uris': list(map((lambda song_id: 'spotify:track:' + song_id), song_ids_to_add))}

	r = requests.delete('https://api.spotify.com/v1/users/{}/playlists/{}/tracks'.format(config['DEFAULT']['MY_SPOTIFY_USER'], playlist_id), headers=req_header, json=req_body)
	if r.status_code in [200, 201]:
		logging.info("Successfully deleted songs {} from playlist {}.".format(song_ids_to_add, playlist_id)) 	
	else:
		logging.warning("Failed to delete songs {} to playlist {}.".format(song_ids_to_add, playlist_id))				
	
# ====================== BEGIN SCRIPT ========================

# read config file
config = configparser.ConfigParser()

# name of the config file is passed as the first command line argument
config.read(str(sys.argv[1]))

# start writing into the log file
now = datetime.datetime.now()
current_date = now.strftime("%Y-%m-%d")

logging.basicConfig(filename="{}ScriptGenerator{}.log".format(config['DEFAULT']['SCRIPT_LOCATION'],current_date),level=logging.DEBUG)
logging.info("Started generating the playlist.")  

# connect to the database
cnxn = pyodbc.connect(config['DEFAULT']['DATABASE_CONNECTION'])

cursor = cnxn.cursor()

# fetch Spotify access token 
cursor.execute("SELECT value FROM tokens WHERE token_type = 'access_token'")
spotify_token = cursor.fetchone()[0]


# first, test current access token
test_request = requests.get('https://api.spotify.com/v1/me', headers=auth_header())
# if unauthorized, need to refresh access token
if test_request.status_code in [401, 403]:
	spotify_token = get_new_access_token()

# populate variables needed to send requests to Spotify and LastFM
user_id = config['DEFAULT']['SPOTIFY_USER']
playlist_id = config['DEFAULT']['PLAYLIST_ID']
api_url = config['DEFAULT']['LASTFM_URL']
api_key = config['DEFAULT']['LASTFM_KEY']

# get songs from the original playlist 
r = requests.get('https://api.spotify.com/v1/users/{}/playlists/{}'.format(user_id,playlist_id), headers=auth_header())
playlist_object = r.json()

# populate data about the playlist and tracks
tracks_object = playlist_object['tracks']
populate_playlist(playlist_object)

for x in range(0, tracks_object['total']):
	if x != 69:
		populate_track(tracks_object['items'][x]['track'])
		populate_playlist_track(tracks_object['items'][x],playlist_object['id'])
	
# populate data about artists and genres
# pick artist_ids from tracks that do not have an entry in the artists table yet
sql_query = """WITH artist_table (artist_id_comb)
				AS (
					SELECT DISTINCT t.ARTIST_ID
					FROM track t
					WHERE t.ARTIST_ID IS NOT NULL
					
					UNION ALL
					
					SELECT DISTINCT t.ARTIST_ID2
					FROM track t
					WHERE t.ARTIST_ID2 IS NOT NULL
					
					UNION ALL
					
					SELECT DISTINCT t.ARTIST_ID3
					FROM track t
					WHERE t.ARTIST_ID3 IS NOT NULL
					
					UNION ALL
					
					SELECT DISTINCT t.ARTIST_ID4
					FROM track t
					WHERE t.ARTIST_ID4 IS NOT NULL
					)
				SELECT artist_id_comb
				FROM artist_table
				LEFT JOIN ARTIST a ON a.ARTIST_ID = artist_table.artist_id_comb
				WHERE a.ARTIST_ID IS NULL"""
				
rows = cursor.execute(sql_query).fetchall()

# for each artist populate data about artist, genre
for row in rows:
	r = requests.get('https://api.spotify.com/v1/artists/{}'.format(row[0]), headers=auth_header())
	artist_object = r.json()
	populate_artist(artist_object)
	# Spotify does not always have all genre data, so checking against LastFM
	populate_genre(artist_object)
	popolate_genre_lastfm(row[0],artist_object['name'])

# query picks up tracks from the original playlist that were added to that playlist during the week	
# query filters playlist tracks based on the chosen music genres
cursor.execute("""SELECT DISTINCT t.TRACK_ID
					FROM PLAYLIST_TRACK pt
					JOIN TRACK t ON pt.TRACK_ID = t.TRACK_ID
					LEFT JOIN ARTIST a ON a.ARTIST_ID = t.ARTIST_ID
						OR a.ARTIST_ID = t.ARTIST_ID2
						OR a.ARTIST_ID = t.ARTIST_ID3
						OR a.ARTIST_ID = t.ARTIST_ID4
					JOIN ARTIST_GENRE ag ON a.ARTIST_ID = ag.ARTIST_ID
					WHERE (
							UPPER(ag.GENRE_ID) LIKE UPPER('%RAP%')
							OR UPPER(ag.GENRE_ID) LIKE UPPER('%HIPHOP%')
							OR UPPER(ag.GENRE_ID) LIKE UPPER('%HIP-HOP%')
							OR UPPER(ag.GENRE_ID) LIKE UPPER('%dancehall%')
							OR UPPER(ag.GENRE_ID) LIKE UPPER('%reggae%')
							OR UPPER(ag.GENRE_ID) LIKE UPPER('%ukdrill%')
							OR UPPER(ag.GENRE_ID) LIKE UPPER('%afrobeat%')
							OR UPPER(ag.GENRE_ID) LIKE UPPER('%grime%')
							)
						AND pt.ADDED_AT > GETDATE() - 7
						AND pt.playlist_id = ?
					""",(playlist_id))
songs_to_add = []	

# use the track list to create or update the playlist based on the information in the config file
for song_id in cursor.fetchall():
	songs_to_add.append(song_id[0])

if config['DEFAULT']['POPULATE_EXISTING_PLAYLIST'] == 'False':
	create_playlist(list(set(songs_to_add))) 
else:
	r = requests.get('https://api.spotify.com/v1/users/{}/playlists/{}'.format(config['DEFAULT']['MY_SPOTIFY_USER'],config['DEFAULT']['PLAYLIST_ID_POPULATED']), headers=auth_header())
	playlist_object = r.json()
	tracks_object = playlist_object['tracks']
	# delete songs from the existing playlist before adding new ones
	songs_to_delete = []	
	for x in range(0, tracks_object['total']):
		songs_to_delete.append(tracks_object['items'][x]['track']['id'])
		
	delete_tracks_from_playlist(config['DEFAULT']['PLAYLIST_ID_POPULATED'],songs_to_delete)	
	add_tracks_to_playlist(config['DEFAULT']['PLAYLIST_ID_POPULATED'],songs_to_add)			

cnxn.close()
logging.info("Finished generating the playlist.")  