CREATE table if not exists top_50_arg_songs(
    song_name text,
    artist text,
    rank int,
    extract_date DATE,
    PRIMARY KEY(rank, extract_date)
);

CREATE table if not exists my_song_history(
    song_name text,
    album text,
    artist text,
    duration_sec int,
    played_at TIMESTAMP WITHOUT TIME ZONE,
    PRIMARY KEY(played_at)
);