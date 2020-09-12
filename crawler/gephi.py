# Wujie Duan and Xinyi Wang are responsible for this code
import queue
import time
import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pylast

cid = "505e9b0ac92f48d6a0159f0de5bc4307" 
secret = "537f00eaee0545318ce903a64ba68c67"

client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# Change data to the format that Gephy takes.
def generate_artist_csv():
    # Ten starting artists
    collection = ['06HL4z0CvFAxyc27GXpf02', 
                    '5nbhd53CLcjfp7U8NDDzHw',
                    '2YF8oM4cDvaP7tCpe4KQvi', 
                    '53deL58uye6fCqRK4COtR4', 
                    '4NJhFmfw43RLBLjQvxDuRS', 
                    '00FQb4jTyendYWaN8pK0wa',
                    '2elBjNSdBE2Y3f0j1mjrql',
                    '2Tglaf8nvDzwSQnpSrjLHP',
                    '2QcZxAgcs2I1q7CtCkl6MI',
                    '2oNStf3CKKLM5lnzELWMcH']
 
    # Change all the artists into node and add an edge between two related artists
    edge = {'start':[], 'end':[], 'weight':[]}
    node = {'artist_id':[], 'artist_name':[], 'cluster_number': [], 'artist_followers':[], 'artist_genres':[]}
    for i in range(10):
        artist_id = collection[i]
        remove_list = []

        # Get all related artists with snowball sampling
        q = queue.Queue()
        q.put_nowait(artist_id)
        while not q.empty():
            time.sleep(0.1)
            if q.qsize() >= 100000:
                break
            artist_id = q.get_nowait()
            remove_list.append(artist_id)

            # Convert each "relation" into connections between nodes
            for related_artist in sp.artist_related_artists(artist_id)['artists']:
                related_artist_id = related_artist['uri'].split(":")[-1]
                edge['start'].append(artist_id)
                edge['end'].append(related_artist_id)
                edge['weight'].append(1)
                q.put_nowait(related_artist_id)

        # Get the union set
        all_artist = set(remove_list) | set(q.queue)
        for n in all_artist:
            time.sleep(0.1)
            info = sp.artist(n)
            node['artist_id'].append(n)
            node['artist_name'].append(info['name'])
            node['cluster_number'].append(i + 1)
            node['artist_followers'].append(info['followers']['total'])
            node['artist_genres'].append(info['genres'])

        # Convert the processed data into csv files, one for edges and one for nodes. 
        df = pd.DataFrame(edge)
        df.to_csv('/home/wd633/spotifycrawler/' + str(i + 1) + '-edge.csv')
        #files.download(str(i + 1) + '-edge.csv')

        df = pd.DataFrame(node)
        df.to_csv('/home/wd633/spotifycrawler/' + str(i + 1) + '-node.csv')
        #files.download(str(i + 1) + '-node.csv')

generate_artist_csv()