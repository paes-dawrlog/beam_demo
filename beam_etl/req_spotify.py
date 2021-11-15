import spotipy, io, configparser
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd

config = configparser.ConfigParser()
config.read('config.cfg')

cid = config['spotify']['cid']
tok = config['spotify']['tok']
client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=tok)
sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)


def call_playlist(creator, playlist_id):
    
    #step1

    playlist_features_list = ["artist","album","track_name",  "track_id","danceability","energy","key","loudness","mode", "speechiness","instrumentalness","liveness","valence","tempo", "duration_ms","time_signature"]
    
    playlist_df = pd.DataFrame(columns = playlist_features_list)
    
    #step2
    
    playlist = sp.user_playlist_tracks(creator, playlist_id)["items"]
    for track in playlist:
        # Create empty dict
        playlist_features = {}
        # Get metadata
        playlist_features["artist"] = track["track"]["album"]["artists"][0]["name"]
        playlist_features["album"] = track["track"]["album"]["name"]
        playlist_features["track_name"] = track["track"]["name"]
        playlist_features["track_id"] = track["track"]["id"]
        
        # Get audio features
        audio_features = sp.audio_features(playlist_features["track_id"])[0]
        for feature in playlist_features_list[4:]:
            playlist_features[feature] = audio_features[feature]
        
        # Concat the dfs
        track_df = pd.DataFrame(playlist_features, index = [0])
        playlist_df = pd.concat([playlist_df, track_df], ignore_index = True)

    #Step 3
        
    return playlist_df

# Available columns Data dictionary available at https://developer.spotify.com/documentation/web-api/reference/#/operations/get-several-audio-features

# ['artist', 'album', 'track_name', 'track_id', 'danceability', 'energy', \ 
# 'key', 'loudness', 'mode', 'speechiness', 'instrumentalness', 'liveness', \
# 'valence', 'tempo', 'duration_ms', 'time_signature']

#tech house 
tracks = call_playlist('spotify', '37i9dQZF1DX6J5NfMJS675')
# tracks = call_playlist('mlatthalin', '4yKY1bEvEhR5w9YZi0IuKk')
tracks['mode'].replace([0,1], ['Minor','Major'], inplace=True)
tracks['key'].replace([0,1,2,3,4,5,6,7,8,9,10,11], ['C','C#|Dm','D','D#|Em','E','F','F#|Gm','G','G#|Am','A','A#|Bm','B'], inplace=True)
# tracks['valence_factor'] = (tracks['valence'].mask(tracks['valence'].gt(0.7), 'Uplifting', inplace=True) | tracks['valence'].mask(tracks['valence'].le(0.4), 'Depressing', inplace=True) | tracks['valence'].mask(tracks['valence'].between(0.4, 0.7), 'Balanced', inplace=True))

print(tracks[['artist', 'album', 'track_name', 'energy', 'key', 'mode', 'speechiness', 'instrumentalness', 'valence', 'tempo']])
