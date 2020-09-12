# Jieyu Chen is responsible for this code
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import pylast
import time

cid = "505e9b0ac92f48d6a0159f0de5bc4307" 
secret = "537f00eaee0545318ce903a64ba68c67"

client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

def get_playlist_audio_features(ids, sp): # enter a list of IDs of songs, and get a list of audio features of each song.
    audio_features = []
    for i in range(len(ids)):
        time.sleep(0.1)
        # to prevent violating the rate limit
        audio_features += sp.audio_features(ids[i])

    features_list = []
    for features in audio_features:
        if features:
            features_list.append([features["energy"], features['liveness'],
                                    features['tempo'], features['speechiness'],
                                    features['acousticness'], features['instrumentalness'],
                                    features['time_signature'], features['danceability'],
                                    features['key'], features['duration_ms'],
                                    features['loudness'], features['valence'],
                                    features['mode'], features['type'],
                                    features['uri']])
        else:
            features_list.append([None]*15)
    return features_list

def get_artist(name):
    results = sp.search(q='artist:' + name, type='artist')
    items = results['artists']['items']
    if len(items) > 0:
        return items[0]
    else:
        return None

def get_album_features(urn):   
    album = sp.album(urn)
    return album

def get_track_features(urn):   # show the track features. The song's country inforation can be extracted from the first two letters of external_ids, the country code.
    track = sp.track(urn)
    return track

# You have to have your own unique two values for API_KEY and API_SECRET
# Obtain yours from https://www.last.fm/api/account/create for Last.fm
API_KEY = "c9a89f41456b4d5337caa8f2d63b8cab"  # this is a sample key
API_SECRET = "9c1a54ac0bdaf31a5f43712d8e47c156"

# In order to perform a write operation you need to authenticate yourself
username = "OsbornCCC"
password_hash = pylast.md5("mlproject12345***")

network = pylast.LastFMNetwork(api_key=API_KEY, api_secret=API_SECRET,
                               username=username, password_hash=password_hash)


def get_features(ids, artists_names, song_names, sp):
    data = []
    audio_feature_lists = get_playlist_audio_features(ids, sp)
    for i in range(len(ids)):
        data.append(dict({'energy':audio_feature_lists[i][0], 
                        'liveness':audio_feature_lists[i][1],
                        'tempo':audio_feature_lists[i][2],
                        'speechiness':audio_feature_lists[i][3],
                        'acousticness':audio_feature_lists[i][4],
                        'instrumentalness':audio_feature_lists[i][5],
                        'time_signature':audio_feature_lists[i][6],
                        'danceability':audio_feature_lists[i][7],
                        'key':audio_feature_lists[i][8],
                        'duration_ms':audio_feature_lists[i][9],
                        'loudness':audio_feature_lists[i][10],
                        'valence':audio_feature_lists[i][11],
                        'mode':audio_feature_lists[i][12]
                        }))
    for j in range(len(ids)):
        data[j]['artist_name'] = artists_names[j]
        data[j]['song_name'] = song_names[j]
        # to prevent violating the rate limit
        time.sleep(0.1)
        artist_name = artists_names[j]

        artist_info = get_artist(artist_name)
        if not artist_info:
            data[j]['artist_followers'] = None
            data[j]['artist_popularity'] = None
            data[j]['artist_genres'] = None
        else:
            data[j]['artist_followers'] = artist_info['followers']['total']
            data[j]['artist_popularity'] = artist_info['popularity']
            data[j]['artist_genres'] = artist_info['genres']
          

        track_info = get_track_features('spotify:track:' + ids[j])
        if track_info:
            data[j]['disc_number'] = track_info['disc_number']
            data[j]['track_id'] = track_info['id']
            data[j]['duration_ms'] = track_info['duration_ms']
            data[j]['explicit'] = track_info['explicit']
            try:
                data[j]['country'] = track_info['external_ids']['isrc'][0:2]
            except:
                data[j]['country'] = None
            try:
                data[j]['album_release_date'] = track_info['album']['release_date']
            except:
                data[j]['album_release_date'] = None
            try:
                data[j]['album_total_tracks'] = track_info['album']['total_tracks']
            except:
                data[j]['album_total_tracks'] = None
            try:
                data[j]['album_available_markets'] = len(track_info['album']['available_markets'])
            except:
                data[j]['album_available_markets'] = None
        else:
            data[j]['disc_number'] = None
            data[j]['track_id'] = None
            data[j]['duration_ms'] = None
            data[j]['explicit'] = None
            data[j]['country'] = None
 
            data[j]['album_release_date'] = None
            data[j]['album_total_tracks'] = None
            data[j]['album_available_markets'] = None
          
        try:
            track = network.get_track(artists_names[j], song_names[j])
            data[j]['listener_count'] = track.get_listener_count()
            data[j]['play_count'] = track.get_playcount()
        except:
            data[j]['listener_count'] = None
            data[j]['play_count'] = None
    
    df = pd.DataFrame(data)
    cols = list(df.columns)
    cols = [cols[-1]] + cols[:-1]
    df = df[cols]
    return df

def txt_to_list(file_name):
    file = open(file_name, 'r')
    lines = file.readlines()
    id_list = []
    name_list = []
    artist = []
    for line in lines:
        line = line.strip('\n').split('\t')
        id_list.append(line[0])
        name_list.append(line[1])
        artist.append(line[2])

    return (id_list[50000:100000], name_list[50000:100000], artist[50000:100000])

txtlist = ["2elBjNSdBE2Y3f0j1mjrql"]
for i in txtlist:   
    song_id, song_name, artist_name = txt_to_list("/home/wd633/spotifycrawler/"+i+".txt")
    print('first step')
    print(len(song_id),len(song_name),len(artist_name))
    df = get_features(song_id, artist_name, song_name, sp)
    df.to_csv("/home/wd633/spotifycrawler/music_data_"+i+"_third.csv")