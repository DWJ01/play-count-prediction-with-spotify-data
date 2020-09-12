import time
import queue
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd

cid ="505e9b0ac92f48d6a0159f0de5bc4307" 
secret = "537f00eaee0545318ce903a64ba68c67"

client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

#Jieyu Chen is responsible for this part of code
def get_playlist_audio_features(ids, sp): # enter a list of IDs of songs, and get a list of audio features of each song.
    audio_features = []
    for i in range(len(ids)):
        audio_features += sp.audio_features(ids[i])

    features_list = []
    for features in audio_features:
        features_list.append([features['energy'], features['liveness'],
                              features['tempo'], features['speechiness'],
                              features['acousticness'], features['instrumentalness'],
                              features['time_signature'], features['danceability'],
                              features['key'], features['duration_ms'],
                              features['loudness'], features['valence'],
                              features['mode'], features['type'],
                              features['uri']])

    df = pd.DataFrame(features_list, columns=['energy', 'liveness',
                                              'tempo', 'speechiness',
                                              'acousticness', 'instrumentalness',
                                              'time_signature', 'danceability',
                                              'key', 'duration_ms', 'loudness',
                                              'valence', 'mode', 'type', 'uri'])
    return audio_features

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

#Wujie Duan and Xinyi Wang are responsible for this part of code
def get_all_artist(artist_id):
    q = queue.Queue()
    q.put_nowait(artist_id)
    while not q.empty():
        time.sleep(0.1)
        if q.qsize() >= 10000:
            break
        artist_id = q.get_nowait()
        for related_artist in sp.artist_related_artists(artist_id)['artists']:
            q.put_nowait(related_artist['uri'].split(":")[-1])
    all_artist = set(q.queue)
    return all_artist

def getTotalTrack(artist_id):
    album_list = set()
    for item in sp.artist_albums(artist_id)['items']:
        album_list.add(item['id'])
    
    track_set = set()
    for album in album_list:
        time.sleep(0.15)
        for track in sp.album_tracks(album)['items']:
            track_set.add((track['id'], track['name'], track['artists'][0]['name']))
    return track_set

artists = ['06HL4z0CvFAxyc27GXpf02', 
           '5nbhd53CLcjfp7U8NDDzHw',
           '2YF8oM4cDvaP7tCpe4KQvi', 
           '53deL58uye6fCqRK4COtR4', 
           '4NJhFmfw43RLBLjQvxDuRS', 
           '00FQb4jTyendYWaN8pK0wa',
           '2elBjNSdBE2Y3f0j1mjrql',
           '2Tglaf8nvDzwSQnpSrjLHP',
           '2QcZxAgcs2I1q7CtCkl6MI',
           '2oNStf3CKKLM5lnzELWMcH']

common_track = set()
for artist_id in artists:
    time.sleep(0.1)
    all_artist = get_all_artist(artist_id)
    all_track = set()
    for a in all_artist:
        all_track = all_track|getTotalTrack(a)
    common_track = common_track & all_track
    file=open('/home/wd633/spotifycrawler/'+artist_id+".txt","w")
    for t in all_track:
        file.write(t[0] + '\t' + t[1] + '\t' + t[2] + '\n')
    file.close()

big_file=open("/home/wd633/spotifycrawler/duplicate.txt","w")
for t in common_track:
    big_file.write(t[0] + '\t' + t[1] + '\t' + t[2] + '\n')
big_file.close()