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
	# update token in db
	cursor.execute("UPDATE tokens SET value = ? WHERE token_type = 'access_token'", (new_token))
	logging.info("Generated new access token.") 
	cnxn.commit()

	return new_token

# populate genre table based on artist object from Spotify
def populate_genre(artist_object):
	try:
		artist_id = artist_object['id']
		genre_num = len(artist_object['genres'])
		for genre in artist_object['genres']:
			genre_id = genre.upper().replace(" ", "")
			genre_name = genre
	 
			cursor.execute("SELECT count(*) FROM GENRE WHERE GENRE_ID = ?", (genre_id))
			genreExists = cursor.fetchone()[0]

			if genreExists == 0:
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
		logging.warning("Was not able to insert data for artist and genre.") 		

# populate genre table based on information from LastFM
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
		
	
	
# ====================== BEGIN SCRIPT ========================

# read config values
config = configparser.ConfigParser()
config.read('config.ini')

# start writing into the log file
now = datetime.datetime.now()
currentDate = now.strftime("%Y-%m-%d")

logging.basicConfig(filename="{}UpdatingGenresLastFm{}.log".format(config['DEFAULT']['SCRIPT_LOCATION'],currentDate),level=logging.DEBUG)
logging.info("Started to update genres.")  

# connect to database
cnxn = pyodbc.connect(config['DEFAULT']['DATABASE_CONNECTION'])

cursor = cnxn.cursor()

# fetch Spotify access token 
cursor.execute("SELECT value FROM tokens WHERE token_type = 'access_token'")
spotify_token = cursor.fetchone()[0]


# first, test current access token
testRequest = requests.get('https://api.spotify.com/v1/me', headers=auth_header())
# if unauthorized, need to refresh access token
if testRequest.status_code in [401, 403]:
	spotify_token = get_new_access_token()

# identify artists that do not have genre assigned
sql_query = """SELECT DISTINCT a.ARTIST_ID
					,a.artist_name
				FROM ARTIST a
				LEFT JOIN ARTIST_GENRE ag ON a.ARTIST_ID = ag.ARTIST_ID
				WHERE ag.ARTIST_ID IS NULL"""

rows = cursor.execute(sql_query).fetchall()

# send request to Spotify to pick up artist's genres
for row in rows:
	r = requests.get('https://api.spotify.com/v1/artists/{}'.format(row[0]), headers=auth_header())
	artist_object = r.json()
	# populate genre table
	populate_genre(artist_object)

# send request to LastFM to pick up artist's genres	
api_url = config['DEFAULT']['LASTFM_URL']
api_key = config['DEFAULT']['LASTFM_KEY']

for row in rows:	
	artist_name = row[1]
	artist_id = row[0]
	try:
		request = '/2.0/?method=artist.getinfo&artist={}&api_key={}&format=json'.format(artist_name,api_key)
		r = requests.get(api_url + request)
		lfreq = r.json()

		for x in range(0,len(lfreq['artist']['tags']['tag'])):
			genre = lfreq['artist']['tags']['tag'][x]['name']
			# populate genre table
			populate_genre_table(artist_id, artist_name, genre)
	except:		
		logging.warning(sys.exc_info())	
		logging.warning(lfreq['error'])	
cnxn.close()
logging.info("Finished updating genres.")  