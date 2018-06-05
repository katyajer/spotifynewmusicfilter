import pyodbc 
import requests
import sys
import datetime
import logging
import configparser

# ====================== METHOD DEFINITIONS ========================
def authHeader():
    return {'Authorization': 'Bearer {}'.format(spotifyToken)}

# obtain new Spotify access token using refresh_token
def getNewAccessToken():
	basicToken = config['DEFAULT']['ENCODED_BASIC_TOKEN']
	refreshToken = config['DEFAULT']['REFRESH_TOKEN']

	reqHeader = {'Authorization': 'Basic {}'.format(basicToken)}
	reqBody = {'grant_type': 'refresh_token', 'refresh_token': refreshToken}
	r = requests.post('https://accounts.spotify.com/api/token', headers=reqHeader, data=reqBody)
	resJson = r.json()
		
	newToken = resJson['access_token']
	# update token in db
	cursor.execute("UPDATE tokens SET value = ? WHERE token_type = 'access_token'", (newToken,))
	logging.info("Generated new access token.") 
	cnxn.commit()

	return newToken
	
# Populates Playlist details based on the Playlist object
def populatePlaylist(playlistObject):
	PLAYLIST_NAME = playlistObject['name']
	PLAYLIST_ID = playlistObject['id']
	COLLABORATIVE = playlistObject['collaborative']	
	SNAPSHOT_ID = playlistObject['snapshot_id']
	Description = playlistObject['description']
	HREF = playlistObject['href']
	PUB = playlistObject['public']
	URI = playlistObject['uri']
	USER_ID = playlistObject['owner']['id']	
	try:        
		cursor.execute("INSERT INTO PLAYLIST (PLAYLIST_NAME, PLAYLIST_ID, COLLABORATIVE, SNAPSHOT_ID, Description, HREF, PUB, URI, USER_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (PLAYLIST_NAME, PLAYLIST_ID, COLLABORATIVE, SNAPSHOT_ID, Description, HREF, PUB, URI, USER_ID))
		logging.info("Created new entry for playlist {}.".format(PLAYLIST_ID)) 
		cnxn.commit()
	except pyodbc.IntegrityError:
		logging.warning("Violation of PRIMARY KEY constraint for playlist {}.".format(PLAYLIST_ID))
	except: 
		logging.warning(sys.exc_info())
		logging.warning("Was not able to insert data for playlist {}.".format(PLAYLIST_ID)) 
		
# Populates Playlist Track based on the Playlist object
def populatePlaylistTrack(playlistTrack, playlistId):
	PLAYLIST_ID = playlistId
	TRACK_ID = playlistTrack['track']['id']	
	IS_LOCAL = playlistTrack['is_local']
	ADDED_BY = playlistTrack['added_by']['id']
	ADDED_AT = playlistTrack['added_at']
	try:        
		cursor.execute("INSERT INTO PLAYLIST_TRACK (PLAYLIST_ID, TRACK_ID, IS_LOCAL, ADDED_BY, ADDED_AT) VALUES (?,?,?,?,?)", (PLAYLIST_ID, TRACK_ID, IS_LOCAL, ADDED_BY, ADDED_AT))
		logging.info("Created new entry for playlist-track {} for playlist {}.".format(playlistTrack['track']['name'], PLAYLIST_ID)) 
		cnxn.commit()
	except pyodbc.IntegrityError:
		logging.warning("Violation of PRIMARY KEY constraint for track {} in playlist {}.".format(playlistTrack['track']['name'], PLAYLIST_ID))
	except: 
		logging.warning(sys.exc_info())
		logging.warning("Was not able to insert data for track {} in playlist {}.".format(playlistTrack['track']['name'], PLAYLIST_ID)) 
		
def populateTrack(trackObject):
	TRACK_NAME = trackObject['name']
	TRACK_ID = trackObject['id']
	duration_ms = trackObject['duration_ms']
	EXPLICIT = trackObject['explicit']
	HREF = trackObject['href']
	POPULARITY = trackObject['popularity']
	URI = trackObject['uri']
	ALBUM_ID = trackObject['album']['id']
	ARTIST_NUM = len(trackObject['artists'])
	ARTIST_ID = trackObject['artists'][0]['id']
	ARTIST_ID2 = None
	ARTIST_ID3 = None
	ARTIST_ID4 = None
	if ARTIST_NUM > 2:
		ARTIST_ID2 = trackObject['artists'][1]['id']
	if ARTIST_NUM > 3:
		ARTIST_ID3 = trackObject['artists'][2]['id']
	if ARTIST_NUM > 4:
		ARTIST_ID4 = trackObject['artists'][3]['id']
	try:        
		cursor.execute("INSERT INTO TRACK (TRACK_NAME, TRACK_ID, duration_ms, EXPLICIT, HREF, POPULARITY, URI, ALBUM_ID, ARTIST_ID, ARTIST_ID2, ARTIST_ID3, ARTIST_ID4) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (TRACK_NAME, TRACK_ID, duration_ms, EXPLICIT, HREF, POPULARITY, URI, ALBUM_ID, ARTIST_ID, ARTIST_ID2, ARTIST_ID3, ARTIST_ID4))
		logging.info("Created new entry for track {} by {}.".format(TRACK_NAME, trackObject['artists'][0]['name'])) 
		cnxn.commit()
	except pyodbc.IntegrityError:
		logging.warning("Violation of PRIMARY KEY constraint for track {} by {}.".format(TRACK_NAME, trackObject['artists'][0]['name']))
	except: 
		logging.warning(sys.exc_info())
		logging.warning("Was not able to insert data for track {} by {}.".format(TRACK_NAME, trackObject['artists'][0]['name'])) 	
		
def populateArtist(artistObject):		
	ARTIST_NAME = artistObject['name']
	ARTIST_ID = artistObject['id']
	HREF = artistObject['href']
	POPULARITY = artistObject['popularity']
	URI= artistObject['uri']
	try:        
		cursor.execute("INSERT INTO ARTIST (ARTIST_NAME, ARTIST_ID, HREF, POPULARITY, URI) VALUES (?, ?, ?, ?, ?)", (ARTIST_NAME, ARTIST_ID, HREF, POPULARITY, URI))
		logging.info("Created new entry for atist {}.".format(ARTIST_NAME))  
		cnxn.commit()
	except pyodbc.IntegrityError:
		logging.warning("Violation of PRIMARY KEY constraint for artist {}.".format(ARTIST_NAME))
	except: 
		logging.warning(sys.exc_info())
		logging.warning("Was not able to insert data for artist {}.".format(ARTIST_NAME)) 		
	
def populateGenre(artistObject):
	ARTIST_ID = artistObject['id']
	GENRE_NUM = len(artistObject['genres'])
	for genre in artistObject['genres']:
		GENRE_ID = genre.upper().replace(" ", "")
		GENRE_NAME = genre
		try: 
			cursor.execute("SELECT count(*) FROM GENRE WHERE GENRE_ID = ?", (GENRE_ID))
			genreExists = cursor.fetchone()[0]

			if genreExists == 0:
				cursor.execute("INSERT INTO GENRE (GENRE_ID, GENRE_NAME) VALUES (?, ?)", (GENRE_ID, GENRE_NAME))
				logging.info("Created new entry for genre {}.".format(GENRE_NAME)) 
			cursor.execute("SELECT count(*) FROM ARTIST_GENRE WHERE GENRE_ID = ? and ARTIST_ID = ?", (GENRE_ID, ARTIST_ID))
			genreArtistsExists = cursor.fetchone()[0]
			if genreArtistsExists == 0:
				cursor.execute("INSERT INTO ARTIST_GENRE (GENRE_ID, ARTIST_ID) VALUES (?, ?)", (GENRE_ID, ARTIST_ID))
				logging.info("Created new entry for artist {} genre {}.".format(artistObject['name'], GENRE_NAME)) 
			cnxn.commit()
		except pyodbc.IntegrityError:
			logging.warning("Violation of PRIMARY KEY constraint for artist {} and genre {}.".format(artistObject['name'],GENRE_NAME))
		except: 
			logging.warning(sys.exc_info())
			logging.warning("Was not able to insert data for artist {} and genre {}.".format(artistObject['name'],GENRE_NAME)) 		
			
def createPlaylist(songIds):
	playlistName = config['DEFAULT']['NEW_PLAYLIST_NAME']
	playlistDescription = config['DEFAULT']['NEW_PLAYLIST_DESCRIPTION']
	
	reqHeader = {'Authorization': 'Bearer {}'.format(spotifyToken), 'Content-Type': 'application/json'}
	reqBody = {'name': playlistName, 'description': playlistDescription}
	r = requests.post('https://api.spotify.com/v1/users/{}/playlists'.format(config['DEFAULT']['MY_SPOTIFY_USER']), headers=reqHeader, json=reqBody)
	playlistObject = r.json()
	newPlaylistId = r.json()['id']	

	if r.status_code in [200, 201]:
		#create record in db for new playlist
		populatePlaylist(playlistObject)
		logging.info("Successfully created new playlist {}.".format(r.json()['id'])) 		
		cnxn.commit()
	else:
		logging.warning("Failed to create new playlist.")
	#add tracks to playlist
	addTracksToPlaylist(newPlaylistId, songIds)			

# place tracks with given ids into Spotify playlist with given id and name
def addTracksToPlaylist(playlistId, songIdsToAdd):
	# songIdsToAdd = []
	# for songId in songIds:
		# songIdsToAdd.append(songId[0])

    # send request to add tracks to Spotify playlist
	reqHeader = {'Authorization': 'Bearer {}'.format(spotifyToken), 'Content-Type': 'application/json'}
	reqBody = {'uris': list(map((lambda songId: 'spotify:track:' + songId), songIdsToAdd))}

	r = requests.post('https://api.spotify.com/v1/users/{}/playlists/{}/tracks'.format(config['DEFAULT']['MY_SPOTIFY_USER'], playlistId), headers=reqHeader, json=reqBody)

	if r.status_code in [200, 201]:
		logging.info("Successfully added songs {} to playlist {}.".format(songIdsToAdd, playlistId)) 		
	else:
		logging.warning("Failed to add songs {} to playlist {}.".format(songIdsToAdd, playlistId))
		
def deleteTracksFromPlaylist(playlistId, songIdsToAdd):
	# songIdsToAdd = []
	# for songId in songIds:
		# songIdsToAdd.append(songId[0])

    # send request to add tracks to Spotify playlist
	reqHeader = {'Authorization': 'Bearer {}'.format(spotifyToken), 'Content-Type': 'application/json'}
	reqBody = {'uris': list(map((lambda songId: 'spotify:track:' + songId), songIdsToAdd))}

	r = requests.delete('https://api.spotify.com/v1/users/{}/playlists/{}/tracks'.format(config['DEFAULT']['MY_SPOTIFY_USER'], playlistId), headers=reqHeader, json=reqBody)
	if r.status_code in [200, 201]:
		logging.info("Successfully deleted songs {} from playlist {}.".format(songIdsToAdd, playlistId)) 	
	else:
		logging.warning("Failed to delete songs {} to playlist {}.".format(songIdsToAdd, playlistId))				
	
# ====================== BEGIN SCRIPT ========================

# read config values
config = configparser.ConfigParser()
config.read('config.ini')

# start writing into the log file
now = datetime.datetime.now()
currentDate = now.strftime("%Y-%m-%d")

logging.basicConfig(filename="{}ScriptGenerator{}.log".format(config['DEFAULT']['SCRIPT_LOCATION'],currentDate),level=logging.DEBUG)
logging.info("Started generating the playlist.")  

# connect to SQLServer database for this script
cnxn = pyodbc.connect(config['DEFAULT']['DATABASE_CONNECTION'])

cursor = cnxn.cursor()

# fetch Spotify access token 
cursor.execute("SELECT value FROM tokens WHERE token_type = 'access_token'")
spotifyToken = cursor.fetchone()[0]


# first, test current access token
testRequest = requests.get('https://api.spotify.com/v1/me', headers=authHeader())
# if unauthorized, need to refresh access token
if testRequest.status_code in [401, 403]:
	spotifyToken = getNewAccessToken()


#Get Songs from New Music Friday
user_id = config['DEFAULT']['SPOTIFY_USER']
playlist_id = config['DEFAULT']['PLAYLIST_ID']

r = requests.get('https://api.spotify.com/v1/users/{}/playlists/{}'.format(user_id,playlist_id), headers=authHeader())
playlistObject = r.json()

# Populate the data about the updated playlist
tracksObject = playlistObject['tracks']
populatePlaylist(playlistObject)

for x in range(0, tracksObject['total']):
	populateTrack(tracksObject['items'][x]['track'])
	populatePlaylistTrack(tracksObject['items'][x],playlistObject['id'])
	
#Populate the data about artist and genre
sql_query = """WITH artist_table (artist_id_comb) 
					AS (
					select distinct t.ARTIST_ID from track t
					where t.ARTIST_ID is not null
					union all
					select distinct t.ARTIST_ID2 from track t
					where t.ARTIST_ID2 is not null
					union all
					select distinct t.ARTIST_ID3 from track t
					where t.ARTIST_ID3 is not null
					union all
					select distinct t.ARTIST_ID4 from track t
					where t.ARTIST_ID4 is not null) 
					select artist_id_comb
					from artist_table
					left join ARTIST a
					on a.ARTIST_ID = artist_table.artist_id_comb
					where a.ARTIST_ID is null"""
rows = cursor.execute(sql_query).fetchall()

for row in rows:
	r = requests.get('https://api.spotify.com/v1/artists/{}'.format(row[0]), headers=authHeader())
	artistObject = r.json()
	populateArtist(artistObject)
	populateGenre(artistObject)

cursor.execute("""select distinct t.track_id from ARTIST a
					join ARTIST_GENRE ag
					on a.ARTIST_ID = ag.ARTIST_ID
					join TRACK t 
					on t.ARTIST_ID =  a.ARTIST_ID
					where UPPER(ag.GENRE_ID) like UPPER('%RAP%')
					or UPPER(ag.GENRE_ID) like UPPER('%HIPHOP%')
					or UPPER(ag.GENRE_ID) like UPPER('%HIP-HOP%')
					or UPPER(ag.GENRE_ID) like UPPER('%dancehall%')
					or UPPER(ag.GENRE_ID) like UPPER('%reggae%')
					or UPPER(ag.GENRE_ID) like UPPER('%ukdrill%')
					or UPPER(ag.GENRE_ID) like UPPER('%afrobeat%') 
					or UPPER(ag.GENRE_ID) like UPPER('%grime%')""")
songsToProcess = []	
for songId in cursor.fetchall():
	songsToProcess.append(songId[0])
 
if config['DEFAULT']['POPULATE_EXISTING_PLAYLIST'] == 'False':
	createPlaylist(list(set(songsToProcess))) 
else:
	r = requests.get('https://api.spotify.com/v1/users/{}/playlists/{}'.format(config['DEFAULT']['MY_SPOTIFY_USER'],config['DEFAULT']['PLAYLIST_ID_POPULATED']), headers=authHeader())
	playlistObject = r.json()

	deleteTracksFromPlaylist(config['DEFAULT']['PLAYLIST_ID_POPULATED'],songsToProcess)	
	addTracksToPlaylist(config['DEFAULT']['PLAYLIST_ID_POPULATED'],songsToProcess)			

cnxn.close()
logging.info("Finished generating the playlist.")  