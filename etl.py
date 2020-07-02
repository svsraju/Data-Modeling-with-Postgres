import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']]
    song_data = song_data.iloc[0].values
    song_data[3] = int(song_data[3])
    song_data[4] = float(song_data[4])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]].iloc[0].values)
    artist_data[3] = float(artist_data[3])
    artist_data[4] = float(artist_data[4])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    df['Date_time'] = pd.to_datetime(df['ts'], unit='ms')
    t = df['Date_time'].astype('datetime64[ns]')
    
    # insert time data records
    timestamp = df.Date_time.dt.time.values
    hour = df.Date_time.dt.hour.values
    day = df.Date_time.dt.day.values
    week_of_year = df.Date_time.dt.weekofyear.values
    month = df.Date_time.dt.month.values
    year = df.Date_time.dt.year.values
    weekday = df.Date_time.dt.weekday.values
    
    time_data = [timestamp, hour, day, week_of_year, month, year, weekday]
    column_labels = ["timestamp", "hour", "day", "week_of_year", "month", "year", "weekday"]
    time_dictionary = dict(zip(column_labels,time_data))
    time_df = pd.DataFrame.from_dict(time_dictionary)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        song_select = "SELECT song_id, songs.artist_id FROM songs JOIN artists ON songs.artist_id = artists.artist_id\
                   WHERE songs.title = %s AND artists.name = %s AND songs.duration = %s"
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.registration,str(row.Date_time),row.userId,row.level,songid,artistid,
                         row.sessionId,row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()