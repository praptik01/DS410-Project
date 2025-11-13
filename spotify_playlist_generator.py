#!/usr/bin/env python
# coding: utf-8

# # Import Libraries

# In[ ]:





# In[1]:


import pyspark
import pandas as pd
import numpy as np
import math


# In[2]:


from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType, FloatType, BooleanType, DoubleType
from pyspark.sql.functions import col, column
from pyspark.sql.functions import expr
from pyspark.sql.functions import split
from pyspark.sql import Row
from pyspark.sql import functions as F, types as T
from functools import reduce
from operator import add
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import monotonically_increasing_id


# # Create SparkSessions and SparkContext

# In[3]:



ss = None
cleaned_df = None
mood_centroids = None
mood_assembler = None
proxy_cols = None
dot_udf = None
norm_udf = None
MOOD_CONFIG = None
recommend_for_mood = None

def _initialize_model():
    global ss, cleaned_df, mood_centroids, mood_assembler, proxy_cols, dot_udf, norm_udf
    global MOOD_CONFIG, recommend_for_mood
    if cleaned_df is not None:
        return
    ss=SparkSession.builder.master("local").appName("Spotify Playlist Generator").getOrCreate()


    # In[4]:


    ss.sparkContext.setCheckpointDir("~/scratch")


    # # Read Data

    # In[5]:


    spotify_DF = (ss.read.format("csv")
          .option("header","true")
          .option("inferSchema","false")
          .option("multiline","true")
          .option("quote", '"')
          .option("escape", '"')
          .option("mode","PERMISSIVE")
          .option("columnNameOfCorruptRecord","_corrupt")
          .load("spotify_dataset.csv"))


    # # Clean & Normalize text columns 

    # In[6]:


    # Expected CSV columns (minimum): title, artist, album, release_year, explicit, playlist_title
    expected_cols = ['song', 'Artist(s)','Energy','Genre', 'Album', 'Tempo', 'Loudness (db)', 'Popularity', 'Liveness', 'Acousticness', 'Speechiness', 'Danceability', 'Positiveness', 'Time signature', 'Instrumentalness', 'Release Date','Explicit']
    for c in expected_cols:
        if c not in spotify_DF.columns:
            raise ValueError(f"Missing expected column: {c}")


    # In[7]:


    def norm(col):
        return F.lower(F.trim(F.regexp_replace(col, r"\s+", " ")))

    df = (
        spotify_DF.withColumn("song_norm", norm(F.col("song")))
          .withColumn("artist_norm", norm(F.col("`Artist(s)`")))
          .withColumn("genre_norm", norm(F.col("Genre")))
          .withColumn("album_norm", norm(F.col("Album")))
    )
    cleaned_df = df.dropDuplicates(["song_norm","artist_norm"])
    if "Loudness (db)" in cleaned_df.columns and "Loudness" not in cleaned_df.columns:
        cleaned_df = cleaned_df.withColumnRenamed("Loudness (db)", "Loudness")


    # # Add a unique ID & drop duplicates

    # In[8]:


    cleaned_df = cleaned_df.fillna({
        "genre_norm": "unknown",
        "artist_norm": "unknown",
        "album_norm": "unknown",
        "song_norm": "unknown"
    })

    cleaned_df = cleaned_df.fillna({
        "Energy": 0.0,
        "Tempo": 0.0,
        "Loudness": 0.0,
        "Popularity": 0,
        "Liveness": 0.0,
        "Acousticness": 0.0,
        "Speechiness": 0.0,
        "Danceability": 0.0,
        "Positiveness": 0.0,
        "Instrumentalness": 0.0,
        "Time signature": 4 
    })

    numeric_regex = r"^\s*-?\d+(\.\d+)?\s*$"

    def safe_cast_float(colname):
        return F.when(F.col(colname).rlike(numeric_regex),
                      F.col(colname).cast("float")) \
                .otherwise(F.lit(None).cast("float"))

    def safe_cast_int(colname):
        return F.when(F.col(colname).rlike(r"^\s*-?\d+\s*$"),
                      F.col(colname).cast("int")) \
                .otherwise(F.lit(None).cast("int"))
    cast_map = {
        "Energy":        "float",
        "Tempo":         "float",
        "Loudness":      "float",
        "Popularity":    "int",
        "Liveness":      "float",
        "Acousticness":  "float",
        "Speechiness":   "float",
        "Danceability":  "float",
        "Positiveness":  "float",
        "Instrumentalness": "float",
        "Time signature":   "int",
    }

    for col_name, target in cast_map.items():
        if col_name in cleaned_df.columns:
            if target == "int":
                cleaned_df = cleaned_df.withColumn(col_name, safe_cast_int(col_name))
            else:
                cleaned_df = cleaned_df.withColumn(col_name, safe_cast_float(col_name))


    # # Handle nulls & fix data types

    # # Keyword flags + coarse genre

    # In[9]:


    cleaned_df = (
        cleaned_df
          .withColumn("song_norm",   norm(F.col("song")))
          .withColumn("artist_norm", norm(F.col("`Artist(s)`")))
          .withColumn("genre_norm",  norm(F.col("Genre")))
          .withColumn("album_norm",  norm(F.col("Album")))
          .withColumn(
              "track_id",
              F.sha2(
                  F.concat_ws(
                      "::",
                      F.coalesce(F.col("song_norm"), F.lit("")),
                      F.coalesce(F.col("artist_norm"), F.lit("")),
                  ),
                  256,
              ),
          )
    )

    txt = F.concat_ws(" | ",
        F.coalesce(F.col("song_norm"), F.lit("")),
        F.coalesce(F.col("artist_norm"), F.lit("")),
        F.coalesce(F.col("album_norm"), F.lit("")),
        F.coalesce(F.col("genre_norm"), F.lit(""))
    )

    def any_rx(col_or_str, pattern):
        c = txt if col_or_str == "txt" else F.col(col_or_str)
        return c.rlike(pattern)

    def none_rx(col_or_str, pattern):
        return ~any_rx(col_or_str, pattern)

    def between(colname, lo, hi):
        return (F.col(colname) >= F.lit(lo)) & (F.col(colname) <= F.lit(hi))

    cleaned_df = (cleaned_df
        .withColumn("kw_remix",        any_rx("txt", r"(?:^|\W)(remix|edit|club|mix)(?:$|\W)"))
        .withColumn("kw_acoustic",     any_rx("txt", r"(?:^|\W)(acoustic|piano|guitar|drums)(?:$|\W)"))
        .withColumn("kw_live",         any_rx("txt", r"(?:^|\W)(unplugged|live)(?:$|\W)"))
        .withColumn("kw_ballad",       any_rx("txt", r"(?:^|\W)(ballad|lullaby)(?:$|\W)"))
        .withColumn("kw_instrumental", any_rx("txt", r"(?:^|\W)(instrumental|lo[\s\-]?fi)(?:$|\W)"))
        .withColumn("kw_christmas",    any_rx("txt", r"(?:^|\W)(christmas|xmas|holiday|noel|mistletoe|santa|merry)(?:$|\W)"))
        .withColumn("kw_musicals",     any_rx("txt", r"(?:^|\W)(original broadway cast|musical|motion picture soundtrack|cast|ensemble)(?:$|\W)"))
        .withColumn("kw_disney",       any_rx("txt", r"(?:^|\W)(disney|pixar)(?:$|\W)"))
        .withColumn("kw_soundtrack",   any_rx("txt", r"(?:^|\W)(soundtrack|score)(?:$|\W)"))
    )

    genre_txt = F.coalesce(
        F.col("genre_norm"),
        F.col("song_norm"),
        F.col("artist_norm"),
        F.col("album_norm"),
        F.lower(F.trim(F.coalesce(F.col("text"), F.lit(""))))
    )

    def g_any_rx(pattern):
        return genre_txt.rlike(pattern)

    cleaned_df = (cleaned_df
      .withColumn("is_holiday",  genre_txt.rlike(r"(christmas|xmas|holiday|noel)"))
      .withColumn("is_disney",   genre_txt.rlike(r"(disney|pixar)"))
      .withColumn("is_score",    genre_txt.rlike(r"(soundtrack|score)"))
      .withColumn("is_lofi",     genre_txt.rlike(r"(lo[\s\-]?fi)"))
      .withColumn("is_latreg",   genre_txt.rlike(r"(reggaeton|reggae|latin)"))
      .withColumn("is_worship",  genre_txt.rlike(r"(christian|worship|gospel)"))
      .withColumn("is_classical",genre_txt.rlike(r"(classical)"))
      .withColumn("is_hiphop",   genre_txt.rlike(r"(hip[\s\-]?hop|rap|trap|grime|cloud rap|emo rap)"))
      .withColumn("is_edm",      genre_txt.rlike(r"(electronic|electro|edm|house|techno|trance|synthpop|electropop|dance|drum and bass|dubstep|dub|chillwave|trip[\s\-]?hop|ambient|chillout)"))
      .withColumn("is_metal",    genre_txt.rlike(r"(metal|heavy metal|death metal|black metal|thrash metal|doom metal|progressive metal|power metal|metalcore|deathcore|hardcore|screamo)"))
      .withColumn("is_rock",     genre_txt.rlike(r"(rock|alternative rock|pop rock|hard rock|classic rock|garage rock|post[\s\-]?punk|punk( rock)?|grunge|britpop|new wave|math rock|shoegaze|psychedelic rock|progressive rock|post[\s\-]?hardcore)"))
      .withColumn("is_indie",    genre_txt.rlike(r"(indie|indie rock|indie pop|dream pop|alternative)"))
      .withColumn("is_folk",     genre_txt.rlike(r"(folk|alt[\s\-]?country|acoustic|country)"))
      .withColumn("is_exper",    genre_txt.rlike(r"(experimental|psychedelic)"))
      .withColumn("is_jazz",     genre_txt.rlike(r"(jazz|soul|funk|blues)"))
      .withColumn("is_pop",      genre_txt.rlike(r"(pop|k[\s\-]?pop|j[\s\-]?pop|dancehall)"))
    )
    cleaned_df = cleaned_df.withColumn(
        "genre_coarse",
        F.when(F.col("is_holiday"),  "holiday") \
         .when(F.col("is_disney"),   "disney") \
         .when(F.col("is_score"),    "soundtrack") \
         .when(F.col("is_lofi"),     "lofi") \
         .when(F.col("is_latreg"),   "latin_reggae") \
         .when(F.col("is_worship"),  "worship") \
         .when(F.col("is_classical"),"classical") \
         .when(F.col("is_hiphop"),   "hiphop") \
         .when(F.col("is_edm"),      "edm") \
         .when(F.col("is_metal"),    "metal") \
         .when(F.col("is_rock"),     "rock") \
         .when(F.col("is_indie"),    "indie") \
         .when(F.col("is_folk"),     "folk") \
         .when(F.col("is_exper"),    "experimental") \
         .when(F.col("is_jazz"),     "jazz_soul") \
         .otherwise("pop")
    )


    # In[10]:


    BASE = {"energy":0.50,"dance":0.50,"loud":0.50,"tempo":0.50,
            "acoustic":0.30,"speech":0.30,"live":0.10,"valence":0.50}
    def litf(x): return F.lit(float(x))
    def clamp01(c): return F.greatest(litf(0.0), F.least(litf(1.0), c))

    # genre increments (fill only what changes; missing = 0.0)
    rows = [
      ("edm",            0.70, 0.70, 0.70, 0.65, 0.00, 0.00, 0.00, 0.00),
      ("hiphop",         0.30, 0.30, 0.30, 0.20, 0.00, 0.15, 0.00, 0.00),
      ("rock",           0.40, 0.20, 0.40, 0.35, 0.00, 0.00, 0.00, 0.00),
      ("metal",          0.50, 0.00, 0.60, 0.30, 0.00, 0.00, 0.00, 0.00),
      ("indie",         -0.10, 0.00, 0.00, 0.00, 0.40, 0.00, 0.00,-0.05),
      ("folk",          -0.10, 0.00, 0.00, 0.00, 0.40, 0.00, 0.00,-0.05),
      ("lofi",          -0.30, 0.10, 0.00, 0.00, 0.20, 0.00, 0.05, 0.00),
      ("soundtrack",    -0.05, 0.00, 0.00, 0.00, 0.00, 0.10, 0.20, 0.00),
      ("disney",         0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.10, 0.20),
      ("holiday",        0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.10, 0.20),
      ("pop",            0.30, 0.30, 0.10, 0.30, 0.00, 0.00, 0.00, 0.00),
      ("classical",      0.10, 0.10, 0.10, 0.20, 0.00, 0.00, 0.00, 0.00),
      ("latin_reggae",   0.40, 0.50, 0.30, 0.50, 0.00, 0.00, 0.00, 0.00),
      ("jazz_soul",     -0.10, 0.10, 0.10, 0.10, 0.40, 0.00, 0.00, 0.00),
      ("worship",       -0.10, 0.10, 0.10, 0.10, 0.20, 0.00, 0.00, 0.00),
      ("experimental",   0.10, 0.10, 0.10, 0.20, 0.00, 0.00, 0.00, 0.00),
    ]
    schema = StructType([
      StructField("genre_coarse", StringType(), False),
      StructField("d_energy", DoubleType(), False),
      StructField("d_dance",  DoubleType(), False),
      StructField("d_loud",   DoubleType(), False),
      StructField("d_tempo",  DoubleType(), False),
      StructField("d_acoustic",DoubleType(), False),
      StructField("d_speech", DoubleType(), False),
      StructField("d_live",   DoubleType(), False),
      StructField("d_valence",DoubleType(), False),
    ])
    genre_bumps = ss.createDataFrame(rows, schema)

    # join bumps
    cleaned_df = cleaned_df.join(genre_bumps, on="genre_coarse", how="left")

    # keyword bumps as simple numeric terms (no extra withColumns)
    kw_energy  = (
        F.when(F.col("kw_remix"), 0.10).otherwise(0.0) + 
        F.when(F.col("kw_acoustic"), -0.20).otherwise(0.0) + 
        F.when(F.col("kw_live"), -0.10).otherwise(0.0) + 
        F.when(F.col("kw_ballad"), -0.10).otherwise(0.0)
    )
    kw_dance   = (F.when(F.col("kw_remix"), 0.10).otherwise(0.0))
    kw_tempo   = (F.when(F.col("kw_remix"), 0.10).otherwise(0.0))
    kw_acoustic= (
        F.when(F.col("kw_acoustic"), 0.30).otherwise(0.0) + 
        F.when(F.col("kw_live"),     0.20).otherwise(0.0)
    )
    kw_live    = (
        F.when(F.col("kw_acoustic"), 0.05).otherwise(0.0) + 
        F.when(F.col("kw_live"),     0.20).otherwise(0.0)
    )
    kw_speech  = (
        F.when(F.col("kw_instrumental"), -0.20).otherwise(0.0) + 
        F.when(F.col("kw_soundtrack"),    0.10).otherwise(0.0)
    )
    kw_valence = (F.when(F.col("kw_ballad"), -0.10).otherwise(0.0))

    # robust Explicit parsing → boolean (no huge casts)
    tok = F.lower(
        F.regexp_extract(F.coalesce(F.col("Explicit").cast("string"), F.lit("")),
                         r"(true|false|1|0|yes|no)", 1)
    )
    explicit_bool = F.when(tok.isin("true","1","yes"), True)                 .when(tok.isin("false","0","no"), False)                 .otherwise(F.lit(False))
    exp_energy  = F.when(explicit_bool, 0.05).otherwise(0.0)
    exp_speech  = F.when(explicit_bool, 0.10).otherwise(0.0)

    # final proxies in ONE projection each (short expressions)
    cleaned_df = (cleaned_df
      .withColumn("energy_proxy",   clamp01(litf(BASE["energy"]) + F.coalesce(F.col("d_energy"),F.lit(0.0)) + kw_energy + exp_energy))
      .withColumn("dance_proxy",    clamp01(litf(BASE["dance"])  + F.coalesce(F.col("d_dance"),F.lit(0.0))   + kw_dance))
      .withColumn("loud_proxy",     clamp01(litf(BASE["loud"])   + F.coalesce(F.col("d_loud"),F.lit(0.0))))
      .withColumn("tempo_proxy",    clamp01(litf(BASE["tempo"])  + F.coalesce(F.col("d_tempo"),F.lit(0.0))   + kw_tempo))
      .withColumn("acoustic_proxy", clamp01(litf(BASE["acoustic"])+F.coalesce(F.col("d_acoustic"),F.lit(0.0))+ kw_acoustic))
      .withColumn("speech_proxy",   clamp01(litf(BASE["speech"]) + F.coalesce(F.col("d_speech"),F.lit(0.0))  + kw_speech + exp_speech))
      .withColumn("live_proxy",     clamp01(litf(BASE["live"])   + F.coalesce(F.col("d_live"),F.lit(0.0))    + kw_live))
      .withColumn("valence_proxy",  clamp01(litf(BASE["valence"])+ F.coalesce(F.col("d_valence"),F.lit(0.0)) + kw_valence))
    )


    # # Clean & Normalize text columns 

    # In[11]:


    cleaned_df = (
        cleaned_df.withColumn("song_norm", norm(F.col("song")))
          .withColumn("artist_norm", norm(F.col("`Artist(s)`")))
          .withColumn("genre_norm", norm(F.col("Genre")))
          .withColumn("album_norm", norm(F.col("Album")))
    )
    cleaned_df.select("song","song_norm","Artist(s)","artist_norm","Genre","genre_norm").show(10, truncate=False)


    # # Hard labels (priority)

    # In[12]:


    POP_IS_0_100 = True   # if popularity already 0-1, set False
    POP_HIGH  = 70.0 if POP_IS_0_100 else 0.70
    POP_MED   = 50.0 if POP_IS_0_100 else 0.50
    def low(column):
        return F.col(column) < 0.3

    def med(column):
        return (F.col(column) >= 0.3) & (F.col(column) < 0.7)

    def high(column):
        return F.col(column) >= 0.7

    def between(column, lower, upper):
        return (F.col(column) >= lower) & (F.col(column) <= upper)

    hard = (
      # --- Era / keyword buckets first (deterministic) ---
      F.when(F.col("song_norm").contains("christmas") | 
               F.col("song_norm").contains("xmas") | 
               F.col("song_norm").contains("holiday") | 
               F.col("song_norm").contains("noel"), 
               F.lit("Christmas"))

       .when(F.col("song_norm").contains("disney") | 
               F.col("song_norm").contains("pixar"), 
               F.lit("Childhood Disney Music"))                                
       .when(F.col("song_norm").contains("musical") | 
               F.col("song_norm").contains("soundtrack") | 
               F.col("song_norm").contains("cast") | 
               F.col("song_norm").contains("ensemble"), 
               F.lit("Musicals"))
       .when(((F.col("release_year") >= 2010) & (F.col("release_year") <= 2019)) &
               ((F.col("genre_norm") == "pop") | 
                (F.col("genre_norm") == "pop punk") | 
                (F.col("genre_norm") == "rock") | 
                (F.col("genre_norm") == "hip hop") | 
                (F.col("genre_norm") == "r&b")), 
               F.lit("2010s Pop Hits"))
       .when((F.col("release_year") >= 2000) & (F.col("release_year") <= 2009),
               F.lit("2000s Throwbacks"))
       .when(((F.col("release_year") >= 1990) & (F.col("release_year") <= 1999)) &
               ((F.col("genre_norm") == "alt") | 
                (F.col("genre_norm") == "grunge") | 
                (F.col("genre_norm") == "rock")),
               F.lit("90s Alt Rock"))
        .when((F.col("release_year") >= 1969) & (F.col("release_year") <= 1989),
               F.lit("Vintage Classics (70-80s)"))

      # Sad girl autumn
       .when(
            (low("Energy") | med("Energy")) &
            between("Tempo", 60, 100) &
            (low("Loudness") | med("Loudness")) &
            none_rx("genre_norm", r"(pop|hip ?hop|rap|edm|house|fast)") &
            any_rx("genre_norm", r"(indie|alt|folk)") &
            low("Liveness") &
            high("Acousticness") &
            low("Speechiness") &
            low("Danceability") &
            low("Positiveness") &
            (F.col("Explicit") == F.lit(False)),
            F.lit("Sad Girl Autumn")
       )

      # Study
       .when(
            (low("Energy") | med("Energy")) &
            between("Tempo", 50, 90) &
            low("Loudness") &
            none_rx("genre_norm", r"(pop|hip ?hop|rap|edm|house|fast)") &
            any_rx("genre_norm", r"(indie|alt ?pop|lo[- ]?fi|jazz)") &
            low("Liveness") &
            high("Acousticness") &
            low("Speechiness") &
            low("Danceability") &
            low("Positiveness") &
            (F.col("Explicit") == F.lit(False)),
            F.lit("Study")
       )

      # Christmas 
       .when(
            (med("Energy") | high("Energy")) &
            between("Tempo", 60, 130) &
            (med("Loudness") | high("Loudness")) &
            (F.col("Popularity") >= POP_HIGH) &
            high("Liveness") &
            high("Acousticness") &
            med("Speechiness") &
            (low("Danceability") | med("Danceability")) &
            high("Positiveness") &
            (F.col("`Time signature`") == 3) &  # 3/4
            high("Instrumentalness") &
            (F.col("Explicit") == F.lit(False)),
            F.lit("Christmas")
       )

      # Road trip
       .when(
            (med("Energy") | high("Energy")) &
            between("Tempo", 60, 100) &
            (med("Loudness") | high("Loudness")) &
            (med("Popularity") | (F.col("Popularity") >= POP_HIGH)) &
            (med("Liveness") | high("Liveness")) &
            med("Danceability") &
            (med("Positiveness") | high("Positiveness")) &
            high("Instrumentalness") &
            (F.col("Explicit") == F.lit(False)),
            F.lit("Road Trip")
       )

      # Driving
       .when(
            (low("Energy") | med("Energy")) &
            between("Tempo", 60, 80) &
            (low("Loudness") | med("Loudness")) &
            low("Liveness") &
            high("Acousticness") &
            low("Speechiness") &
            low("Danceability") &
            low("Positiveness") &
            high("Instrumentalness") &
            (F.col("Explicit") == F.lit(False)),
            F.lit("Driving")
       )

      # Musicals 
       .when(
            (low("Energy") | med("Energy")) &
            (low("Loudness") | med("Loudness") | high("Loudness")) &
            (F.col("Popularity") >= POP_HIGH) &
            (med("Liveness") | high("Liveness")) &
            high("Acousticness") &
            high("Speechiness") &
            (low("Danceability") | med("Danceability") | high("Danceability")) &
            (low("Positiveness") | med("Positiveness") | high("Positiveness")) &
            (F.col("Explicit") == F.lit(False)),
            F.lit("Musicals")
       )

      # 2010s Pop Hits
       .when(
            (med("Energy") | high("Energy")) &
            between("Tempo", 80, 130) &
            (med("Loudness") | high("Loudness")) &
            (F.col("Popularity") >= POP_HIGH) &
            (med("Liveness") | high("Liveness")),
            F.lit("2010s Pop Hits")
       )

      # 90s Alt Rock 
       .when(
            high("Energy") &
            between("Tempo", 110, 140) &
            high("Loudness") &
            (F.col("Popularity") >= POP_HIGH),
            F.lit("90s Alt Rock")
       )

      # Vintage classics 
       .when(
            med("Energy") &
            between("Tempo", 60, 130) &
            (med("Loudness") | high("Loudness")) &
            (F.col("Popularity") >= POP_HIGH) &
            (med("Liveness") | high("Liveness")) &
            high("Acousticness") &
            high("Speechiness") &
            high("Danceability") &
            high("Positiveness"),
            F.lit("Vintage Classics (70-80s)")
       )

      # Childhood Disney Music
       .when(
            between("Tempo", 60, 120) &
            (F.col("Popularity") >= POP_HIGH) &
            (med("Liveness") | high("Liveness")) &
            high("Acousticness") &
            high("Speechiness") &
            high("Danceability") &
            high("Positiveness") &
            high("Instrumentalness") &
            (F.col("Explicit") == F.lit(False)),
            F.lit("Childhood Disney Music")
       )

      # Summer beach day
       .when(
            high("Energy") &
            between("Tempo", 100, 130) &
            high("Loudness") &
            high("Danceability") &
            high("Positiveness") &
            (F.col("Popularity") >= POP_HIGH),
            F.lit("Summer Beach Day")
       )

      # Autumn cozy
       .when(
            (low("Energy") | med("Energy")) &
            between("Tempo", 60, 80) &
            med("Loudness") &
            (low("Danceability") | med("Danceability")) &
            (F.col("Popularity") >= POP_MED),
            F.lit("Autumn Cozy")
       )

      # Campfire nights
       .when(
            med("Energy") &
            between("Tempo", 110, 130) &
            (med("Loudness") | high("Loudness")) &
            (med("Popularity") | (F.col("Popularity") >= POP_HIGH)) &
            (med("Liveness") | high("Liveness")) &
            high("Acousticness") &
            high("Speechiness") &
            med("Danceability") &
            high("Positiveness") &
            (F.col("Explicit") == F.lit(False)),
            F.lit("Campfire Nights")
       )

      # Indie chill
       .when(
            low("Energy") &
            between("Tempo", 70, 100) &
            any_rx("genre_norm", r"(indie|alternative)") &
            (low("Liveness") | med("Liveness")) &
            high("Acousticness") &
            low("Speechiness") &
            low("Danceability") &
            (med("Positiveness") | (F.col("Positiveness") == 0.5)) &
            high("Instrumentalness"),
            F.lit("Indie Chill")
       )

      # City night stroll
       .when(
            low("Energy") &
            between("Tempo", 70, 100) &
            any_rx("genre_norm", r"(ambient|classical|jazz)") &
            (low("Liveness") | med("Liveness")) &
            high("Acousticness") &
            low("Speechiness") &
            low("Danceability") &
            (med("Positiveness") | (F.col("Positiveness") == 0.5)) &
            high("Instrumentalness"),
            F.lit("City Night Stroll")
       )

      # Villain arc gym music
       .when(
            high("Energy") &
            between("Tempo", 110, 150) &
            high("Loudness") &
            (med("Popularity") | (F.col("Popularity") >= POP_HIGH)) &
            (med("Liveness") | high("Liveness")) &
            high("Acousticness") &   # you listed high
            high("Speechiness") &
            high("Danceability") &
            high("Positiveness"),
            F.lit("Villain Arc Gym Music")
       )

      # I’m unbothered era (sassy)
       .when(
            high("Energy") &
            between("Tempo", 100, 130) &
            high("Loudness") &
            (med("Popularity") | (F.col("Popularity") >= POP_HIGH)) &
            (med("Liveness") | high("Liveness")) &
            high("Acousticness") &
            high("Speechiness") &
            high("Danceability") &
            high("Positiveness"),
            F.lit("I’m Unbothered Era (Sassy)")
       )

      # Slow morning
       .when(
            (low("Energy") | med("Energy")) &
            between("Tempo", 60, 80) &
            (low("Loudness") | med("Loudness")) &
            (low("Liveness") | med("Liveness")) &
            high("Acousticness") &
            (low("Speechiness") | med("Speechiness")) &
            low("Danceability") &
            (low("Positiveness") | med("Positiveness")) &
            (F.col("`Time signature`") == 3),
            F.lit("Slow Morning")
       )

      # Post breakup depressed
       .when(
            low("Energy") &
            between("Tempo", 60, 100) &
            (low("Loudness") | med("Loudness")),
            F.lit("Post Breakup Depressed")
       )

      # Midnight existential
       .when(
            (low("Energy") | med("Energy")) &
            between("Tempo", 60, 80) &
            (low("Loudness") | med("Loudness")) &
            high("Liveness") &
            high("Speechiness") &
            high("Danceability") &
            high("Positiveness") &
            high("Instrumentalness"),
            F.lit("Midnight Existential")
       )

      # Hot girl walk
       .when(
            between("Tempo", 60, 130) &
            med("Loudness") &
            (med("Popularity") | (F.col("Popularity") >= POP_HIGH)) &
            high("Liveness") &
            high("Acousticness") &
            (F.col("Speechiness") >= 0.5) &   # "medium"
            (F.col("Danceability") >= 0.5) &  # "medium"
            high("Positiveness") &
            (F.col("`Time signature`") == 3) &  # 3/4
            (F.col("Instrumentalness") >= 0.5),
            F.lit("Hot Girl Walk")
       )

      # Main character energy
       .when(
            high("Energy") &
            between("Tempo", 70, 100) &
            low("Loudness") &
            (med("Popularity") | (F.col("Popularity") >= POP_HIGH)) &
            (med("Liveness") | high("Liveness")) &
            high("Acousticness") &
            high("Speechiness") &
            high("Danceability") &
            high("Positiveness"),
            F.lit("Main Character Energy")
       )

      # Dream / escapist
       .when(
            low("Energy") &
            between("Tempo", 100, 130) &
            high("Loudness") &
            any_rx("genre_norm", r"(experimental|psychedelic|psychedelic rock|dream ?pop)") &
            (F.col("Popularity") >= POP_MED) &
            (low("Liveness") | med("Liveness")) &
            high("Acousticness") &
            low("Speechiness") &
            low("Danceability") &
            low("Positiveness") &
            high("Instrumentalness"),
            F.lit("Dream/Escapist")
       )

      # Going out
       .when(
            (med("Energy") | high("Energy")) &
            between("Tempo", 80, 130) &
            high("Loudness") &
            (F.col("Popularity") >= POP_MED) &
            high("Liveness") &
            (F.col("Acousticness") >= 0.5) &   # "medium"
            (F.col("Speechiness") >= 0.5) &    # "medium-high"
            high("Danceability") &
            high("Positiveness") &
            (F.col("`Time signature`") == 4) &  # 4/4
            (F.col("Instrumentalness") <= 0.4),
            F.lit("Going Out")
       )
       .otherwise(F.lit(None))

    )


    # # Assign categories to existing songs based on label rules

    # In[13]:


    if "hard_label" not in cleaned_df.columns:
        cleaned_df = cleaned_df.withColumn("hard_label", F.lit(None).cast("string"))

    if "soft_label" not in cleaned_df.columns:
        cleaned_df = cleaned_df.withColumn("soft_label", F.lit(None).cast("string"))

    if "soft_score" not in cleaned_df.columns:
        cleaned_df = cleaned_df.withColumn("soft_score", F.lit(0.0).cast("double"))

    # soft_score based on proxy features (genre + keywords + explicit already baked in)
    cleaned_df = cleaned_df.withColumn(
        "soft_score",
        0.30 * F.col("energy_proxy") +
        0.20 * F.col("dance_proxy") +
        0.10 * F.col("tempo_proxy") +
        0.20 * F.col("valence_proxy") +
        0.10 * F.col("live_proxy") +
        0.10 * (1.0 - F.col("acoustic_proxy"))   # less acoustic = more "hype"
    )

    # Turn soft_score into a label
    cleaned_df = cleaned_df.withColumn(
        "soft_label",
        F.when(F.col("soft_score") >= 0.6, "high_energy")
         .when(F.col("soft_score") >= 0.3, "medium_energy")
         .otherwise("low_energy")
    )

    # Final category: hard label wins, then soft label, then coarse genre
    cleaned_df = cleaned_df.withColumn(
        "assigned_category",
        F.coalesce(
            F.col("hard_label"),
            F.col("soft_label"),
            F.col("genre_coarse")
        )
    )


    # # Get Category Centroids

    # In[14]:


    def safe_double(column):
        col_str = F.col(column).cast("string")
        return F.when(
            col_str.rlike(r"^-?\d+(\.\d+)?$"),
            col_str.cast("double")
        ).otherwise(F.lit(None).cast("double"))
    feature_cols = [
        "Energy", "Tempo", "Loudness", "Danceability", "Positiveness",
        "Acousticness", "Speechiness", "Liveness", "Instrumentalness"
    ]

    numeric_df = cleaned_df.select(
        "assigned_category",
        *[safe_double(c).alias(c) for c in feature_cols],
    )

    cat_avgs = (
        numeric_df
            .filter(F.col("assigned_category").isNotNull())
            .groupBy("assigned_category")
            .agg(*[F.avg(c).alias(c) for c in feature_cols])
    )

    cat_avgs = cat_avgs.fillna({c: 0.0 for c in feature_cols})

    #cat_avgs.show(truncate=False)


    # In[15]:


    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df_vectorize = assembler.transform(cat_avgs)

    df_vectorize.select("assigned_category", "features").show(truncate=False)

    a = df_vectorize.select(
            F.col("assigned_category").alias("cat_i"),
            F.col("features").alias("vector1")
        )
    b = df_vectorize.select(
            F.col("assigned_category").alias("cat_j"),
            F.col("features").alias("vector2")
        )

    pairs = a.crossJoin(b)

    def dot(v1, v2):
        if v1 is None or v2 is None:
            return None
        return float(v1.dot(v2))

    def norm(v):
        if v is None:
            return None
        return float(math.sqrt(v.dot(v)))

    dot_udf = F.udf(dot, T.DoubleType())
    norm_udf = F.udf(norm, T.DoubleType())

    cosine_similarity = (
        pairs
          .withColumn("dot_product", dot_udf("vector1", "vector2"))
          .withColumn("norm_1",      norm_udf("vector1"))
          .withColumn("norm_2",      norm_udf("vector2"))
          .withColumn(
              "cosine_similarity",
              F.when(
                  (F.col("norm_1") > 0) & (F.col("norm_2") > 0),
                  F.col("dot_product") / (F.col("norm_1") * F.col("norm_2"))
              ).otherwise(F.lit(None).cast("double"))
          )
    )

    #cosine_similarity.show(truncate=False)


    # # Vectorize songs with same feature set

    # In[16]:


    songs_numeric = cleaned_df.select(
        "track_id",
        "song",
        "Artist(s)",
        "assigned_category",
        *[F.col(c).cast("double").alias(c) for c in feature_cols]
    )
    songs_numeric = songs_numeric.fillna({c: 0.0 for c in feature_cols})

    songs_assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features"
    )

    songs_vec = songs_assembler.transform(songs_numeric)

    songs_vec.select(
        "track_id", "song", "Artist(s)", "assigned_category", "features"
    ).show(5, truncate=False)


    # # Compute per-song similarity to the centroid of its assigned category

    # In[17]:


    centroid_df = df_vectorize.select(
        F.col("assigned_category").alias("cat"),
        F.col("features").alias("centroid_vec")
    )
    songs_with_centroids = (
        songs_vec
          .join(centroid_df, songs_vec.assigned_category == centroid_df.cat, "left")
          .drop("cat")
    )
    def dot(v1, v2):
        if v1 is None or v2 is None:
            return None
        return float(v1.dot(v2))

    def norm(v):
        if v is None:
            return None
        return float(math.sqrt(v.dot(v)))

    dot_udf = F.udf(dot, T.DoubleType())
    norm_udf = F.udf(norm, T.DoubleType())
    songs_scored = (
        songs_with_centroids
            .withColumn("dot",   dot_udf("features", "centroid_vec"))
            .withColumn("n1",    norm_udf("features"))
            .withColumn("n2",    norm_udf("centroid_vec"))
            .withColumn(
                "song_similarity",
                (F.col("dot") / (F.col("n1") * F.col("n2")))
            )
    )

    songs_scored.select(
        "track_id","song","Artist(s)","assigned_category","song_similarity"
    ).show(10, truncate=False)


    # In[18]:


    cleaned_df = cleaned_df.withColumn(
        "year",
        F.regexp_extract("Release Date", r"(\d{4})$", 1).cast("int")
    )

    # explicit → boolean
    cleaned_df = cleaned_df.withColumn(
        "is_explicit",
        (F.col("Explicit") == "Yes").cast("boolean")
    )

    proxy_cols = [
        "energy_proxy",
        "dance_proxy",
        "loud_proxy",
        "tempo_proxy",
        "acoustic_proxy",
        "speech_proxy",
        "live_proxy",
        "valence_proxy",
    ]

    MOOD_CONFIG = {
        # 1. Sad girl autumn
        "sad_girl_autumn": {
            "hard_filters": [
                F.col("soft_label").isin("low_energy", "medium_energy"),
                (F.col("Tempo") >= 60) & (F.col("Tempo") <= 100),
                ~F.col("is_pop") & ~F.col("is_hiphop"),
                (F.col("is_explicit") == False),
            ],
            "targets": {
                "energy_proxy":   0.35,
                "tempo_proxy":    0.35,
                "loud_proxy":     0.3,
                "dance_proxy":    0.25,
                "acoustic_proxy": 0.8,
                "speech_proxy":   0.2,
                "live_proxy":     0.3,
                "valence_proxy":  0.2,
            },
            "weights": {
                "energy_proxy":   1.0,
                "acoustic_proxy": 1.0,
                "valence_proxy":  1.0,
                "tempo_proxy":    0.7,
                "dance_proxy":    0.6,
                "loud_proxy":     0.5,
                "speech_proxy":   0.4,
                "live_proxy":     0.3,
            },
        },

        # 2. Study
        "study": {
            "hard_filters": [
                F.col("soft_label").isin("low_energy", "medium_energy"),
                (F.col("Tempo") >= 50) & (F.col("Tempo") <= 90),
                (F.col("is_explicit") == False),
                ~F.col("is_hiphop"),
            ],
            "targets": {
                "energy_proxy":   0.3,
                "tempo_proxy":    0.3,
                "loud_proxy":     0.2,
                "dance_proxy":    0.2,
                "acoustic_proxy": 0.8,
                "speech_proxy":   0.1,
                "live_proxy":     0.2,
                "valence_proxy":  0.3,
            },
            "weights": {
                "acoustic_proxy": 1.0,
                "speech_proxy":   1.0,
                "energy_proxy":   0.9,
                "tempo_proxy":    0.8,
                "loud_proxy":     0.6,
                "dance_proxy":    0.5,
                "valence_proxy":  0.4,
                "live_proxy":     0.3,
            },
        },

        # 3. 2000s throwbacks
        "throwbacks_2000s": {
            "hard_filters": [
                F.col("soft_label").isin("high_energy", "medium_energy"),
                (F.col("Tempo") >= 80) & (F.col("Tempo") <= 130),
                (F.col("year") >= 2000) & (F.col("year") <= 2009),
                (F.col("is_pop") | F.col("is_hiphop") | F.col("is_rock")),
                (F.col("Popularity") >= 40),
            ],
            "targets": {
                "energy_proxy":   0.8,
                "tempo_proxy":    0.7,
                "loud_proxy":     0.8,
                "dance_proxy":    0.8,
                "acoustic_proxy": 0.3,
                "speech_proxy":   0.4,
                "live_proxy":     0.6,
                "valence_proxy":  0.6,
            },
            "weights": {
                "energy_proxy":   1.0,
                "dance_proxy":    1.0,
                "tempo_proxy":    0.8,
                "loud_proxy":     0.8,
                "valence_proxy":  0.5,
                "live_proxy":     0.4,
                "speech_proxy":   0.4,
                "acoustic_proxy": 0.3,
            },
        },

        # 4. Christmas
        "christmas": {
            "hard_filters": [
                (F.col("is_holiday") | F.col("kw_christmas")),
                (F.col("soft_label").isin("medium_energy", "high_energy")),
                (F.col("Tempo") >= 60) & (F.col("Tempo") <= 130),
                (F.col("Popularity") >= 40),
                (F.col("is_explicit") == False),
            ],
            "targets": {
                "energy_proxy":   0.6,
                "tempo_proxy":    0.6,
                "loud_proxy":     0.6,
                "dance_proxy":    0.5,
                "acoustic_proxy": 0.7,
                "speech_proxy":   0.4,
                "live_proxy":     0.7,
                "valence_proxy":  0.9,
            },
            "weights": {
                "valence_proxy":  1.0,
                "live_proxy":     1.0,
                "acoustic_proxy": 0.8,
                "energy_proxy":   0.7,
                "tempo_proxy":    0.7,
                "dance_proxy":    0.5,
                "loud_proxy":     0.5,
                "speech_proxy":   0.4,
            },
        },

        # 5. Road trip
        "road_trip": {
            "hard_filters": [
                F.col("soft_label").isin("medium_energy", "high_energy"),
                (F.col("Tempo") >= 60) & (F.col("Tempo") <= 100),
                (F.col("Popularity") >= 30),
            ],
            "targets": {
                "energy_proxy":   0.7,
                "tempo_proxy":    0.5,
                "loud_proxy":     0.7,
                "dance_proxy":    0.6,
                "acoustic_proxy": 0.4,
                "speech_proxy":   0.4,
                "live_proxy":     0.6,
                "valence_proxy":  0.7,
            },
            "weights": {
                "energy_proxy":   1.0,
                "valence_proxy":  0.9,
                "tempo_proxy":    0.8,
                "dance_proxy":    0.7,
                "loud_proxy":     0.7,
                "live_proxy":     0.5,
                "acoustic_proxy": 0.3,
                "speech_proxy":   0.3,
            },
        },

        # 6. Driving (chill)
        "driving_mellow": {
            "hard_filters": [
                F.col("soft_label").isin("low_energy", "medium_energy"),
                (F.col("Tempo") >= 60) & (F.col("Tempo") <= 80),
                (F.col("is_explicit") == False),
            ],
            "targets": {
                "energy_proxy":   0.3,
                "tempo_proxy":    0.3,
                "loud_proxy":     0.3,
                "dance_proxy":    0.3,
                "acoustic_proxy": 0.8,
                "speech_proxy":   0.2,
                "live_proxy":     0.2,
                "valence_proxy":  0.3,
            },
            "weights": {
                "acoustic_proxy": 1.0,
                "energy_proxy":   0.8,
                "tempo_proxy":    0.7,
                "valence_proxy":  0.5,
                "dance_proxy":    0.4,
                "loud_proxy":     0.4,
                "speech_proxy":   0.3,
                "live_proxy":     0.3,
            },
        },

        # 7. Musicals
        "musicals": {
            "hard_filters": [
                (F.col("kw_musicals") | F.col("kw_soundtrack") | F.col("is_disney")),
                (F.col("Popularity") >= 30),
                (F.col("is_explicit") == False),
            ],
            "targets": {
                "energy_proxy":   0.5,
                "tempo_proxy":    0.5,
                "loud_proxy":     0.5,
                "dance_proxy":    0.6,
                "acoustic_proxy": 0.7,
                "speech_proxy":   0.7,
                "live_proxy":     0.6,
                "valence_proxy":  0.7,
            },
            "weights": {
                "speech_proxy":   1.0,
                "acoustic_proxy": 0.9,
                "valence_proxy":  0.8,
                "live_proxy":     0.7,
                "dance_proxy":    0.6,
                "energy_proxy":   0.5,
                "tempo_proxy":    0.5,
                "loud_proxy":     0.4,
            },
        },

        # 8. 2010s pop hits
        "pop_hits_2010s": {
            "hard_filters": [
                (F.col("year") >= 2010) & (F.col("year") <= 2019),
                (F.col("is_pop") | F.col("is_rock") | F.col("is_hiphop")),
                (F.col("Tempo") >= 80) & (F.col("Tempo") <= 130),
                (F.col("Popularity") >= 50),
            ],
            "targets": {
                "energy_proxy":   0.8,
                "tempo_proxy":    0.7,
                "loud_proxy":     0.8,
                "dance_proxy":    0.7,
                "acoustic_proxy": 0.3,
                "speech_proxy":   0.4,
                "live_proxy":     0.7,
                "valence_proxy":  0.7,
            },
            "weights": {
                "energy_proxy":   1.0,
                "dance_proxy":    0.9,
                "tempo_proxy":    0.8,
                "loud_proxy":     0.8,
                "valence_proxy":  0.6,
                "live_proxy":     0.5,
                "speech_proxy":   0.4,
                "acoustic_proxy": 0.3,
            },
        },

        # 9. 90s alt rock
        "alt_rock_90s": {
            "hard_filters": [
                (F.col("year") >= 1990) & (F.col("year") <= 1999),
                F.col("is_rock"),
                (F.col("Tempo") >= 110) & (F.col("Tempo") <= 140),
                (F.col("soft_label") == "high_energy"),
                (F.col("Popularity") >= 40),
            ],
            "targets": {
                "energy_proxy":   0.9,
                "tempo_proxy":    0.8,
                "loud_proxy":     0.9,
                "dance_proxy":    0.7,
                "acoustic_proxy": 0.4,
                "speech_proxy":   0.5,
                "live_proxy":     0.8,
                "valence_proxy":  0.3,
            },
            "weights": {
                "energy_proxy":   1.0,
                "loud_proxy":     1.0,
                "live_proxy":     0.9,
                "tempo_proxy":    0.8,
                "dance_proxy":    0.6,
                "speech_proxy":   0.4,
                "acoustic_proxy": 0.3,
                "valence_proxy":  0.3,
            },
        },

        # 10. Vintage classics (70–80s/late 60s)
        "vintage_classics_70s_80s": {
            "hard_filters": [
                (F.col("year") >= 1969) & (F.col("year") <= 1989),
                (F.col("Popularity") >= 40),
            ],
            "targets": {
                "energy_proxy":   0.6,
                "tempo_proxy":    0.6,
                "loud_proxy":     0.7,
                "dance_proxy":    0.8,
                "acoustic_proxy": 0.7,
                "speech_proxy":   0.6,
                "live_proxy":     0.7,
                "valence_proxy":  0.8,
            },
            "weights": {
                "dance_proxy":    1.0,
                "valence_proxy":  0.9,
                "live_proxy":     0.8,
                "acoustic_proxy": 0.7,
                "energy_proxy":   0.7,
                "tempo_proxy":    0.7,
                "speech_proxy":   0.5,
                "loud_proxy":     0.5,
            },
        },

        # 11. Childhood Disney
        "childhood_disney": {
            "hard_filters": [
                (F.col("is_disney") | F.col("kw_disney") | F.col("kw_soundtrack")),
                (F.col("Popularity") >= 30),
                (F.col("is_explicit") == False),
            ],
            "targets": {
                "energy_proxy":   0.6,
                "tempo_proxy":    0.5,
                "loud_proxy":     0.6,
                "dance_proxy":    0.7,
                "acoustic_proxy": 0.7,
                "speech_proxy":   0.7,
                "live_proxy":     0.8,
                "valence_proxy":  0.9,
            },
            "weights": {
                "valence_proxy":  1.0,
                "live_proxy":     0.9,
                "speech_proxy":   0.8,
                "acoustic_proxy": 0.7,
                "dance_proxy":    0.7,
                "energy_proxy":   0.6,
                "tempo_proxy":    0.5,
                "loud_proxy":     0.5,
            },
        },

        # 12. Summer beach day
        "summer_beach_day": {
            "hard_filters": [
                (F.col("soft_label") == "high_energy"),
                (F.col("Tempo") >= 100) & (F.col("Tempo") <= 130),
                (F.col("Popularity") >= 40),
            ],
            "targets": {
                "energy_proxy":   0.9,
                "tempo_proxy":    0.8,
                "loud_proxy":     0.9,
                "dance_proxy":    0.9,
                "acoustic_proxy": 0.3,
                "speech_proxy":   0.6,
                "live_proxy":     0.8,
                "valence_proxy":  0.9,
            },
            "weights": {
                "energy_proxy":   1.0,
                "dance_proxy":    1.0,
                "valence_proxy":  0.9,
                "tempo_proxy":    0.8,
                "loud_proxy":     0.8,
                "live_proxy":     0.7,
                "speech_proxy":   0.5,
                "acoustic_proxy": 0.3,
            },
        },

        # 13. Autumn cozy
        "autumn_cozy": {
            "hard_filters": [
                F.col("soft_label").isin("low_energy", "medium_energy"),
                (F.col("Tempo") >= 60) & (F.col("Tempo") <= 80),
            ],
            "targets": {
                "energy_proxy":   0.4,
                "tempo_proxy":    0.35,
                "loud_proxy":     0.4,
                "dance_proxy":    0.4,
                "acoustic_proxy": 0.6,
                "speech_proxy":   0.3,
                "live_proxy":     0.5,
                "valence_proxy":  0.5,
            },
            "weights": {
                "energy_proxy":   0.9,
                "tempo_proxy":    0.8,
                "acoustic_proxy": 0.7,
                "valence_proxy":  0.6,
                "live_proxy":     0.5,
                "dance_proxy":    0.4,
                "loud_proxy":     0.4,
                "speech_proxy":   0.3,
            },
        },

        # 14. Campfire nights
        "campfire_nights": {
            "hard_filters": [
                (F.col("Tempo") >= 110) & (F.col("Tempo") <= 130),
                (F.col("Popularity") >= 30),
            ],
            "targets": {
                "energy_proxy":   0.6,
                "tempo_proxy":    0.6,
                "loud_proxy":     0.7,
                "dance_proxy":    0.6,
                "acoustic_proxy": 0.8,
                "speech_proxy":   0.6,
                "live_proxy":     0.7,
                "valence_proxy":  0.8,
            },
            "weights": {
                "acoustic_proxy": 1.0,
                "live_proxy":     0.9,
                "valence_proxy":  0.8,
                "energy_proxy":   0.7,
                "tempo_proxy":    0.7,
                "dance_proxy":    0.6,
                "loud_proxy":     0.5,
                "speech_proxy":   0.5,
            },
        },

        # 15. Indie chill
        "indie_chill": {
            "hard_filters": [
                F.col("is_indie") | F.col("is_folk") | F.col("is_rock"),
                F.col("soft_label") == "low_energy",
                (F.col("Tempo") >= 70) & (F.col("Tempo") <= 100),
            ],
            "targets": {
                "energy_proxy":   0.3,
                "tempo_proxy":    0.4,
                "loud_proxy":     0.4,
                "dance_proxy":    0.3,
                "acoustic_proxy": 0.8,
                "speech_proxy":   0.2,
                "live_proxy":     0.4,
                "valence_proxy":  0.5,
            },
            "weights": {
                "acoustic_proxy": 1.0,
                "energy_proxy":   0.9,
                "valence_proxy":  0.7,
                "tempo_proxy":    0.7,
                "loud_proxy":     0.5,
                "dance_proxy":    0.4,
                "live_proxy":     0.4,
                "speech_proxy":   0.3,
            },
        },

        # 16. City night stroll
        "city_night_stroll": {
            "hard_filters": [
                (F.col("is_jazz") | F.col("is_classical") | F.col("is_exper")),
                (F.col("soft_label").isin("low_energy", "medium_energy")),
                (F.col("Tempo") >= 70) & (F.col("Tempo") <= 100),
            ],
            "targets": {
                "energy_proxy":   0.3,
                "tempo_proxy":    0.4,
                "loud_proxy":     0.4,
                "dance_proxy":    0.2,
                "acoustic_proxy": 0.8,
                "speech_proxy":   0.2,
                "live_proxy":     0.4,
                "valence_proxy":  0.5,
            },
            "weights": {
                "acoustic_proxy": 1.0,
                "energy_proxy":   0.8,
                "tempo_proxy":    0.7,
                "valence_proxy":  0.6,
                "live_proxy":     0.5,
                "loud_proxy":     0.4,
                "dance_proxy":    0.4,
                "speech_proxy":   0.3,
            },
        },

        # 17. Villain arc gym music
        "villain_arc_gym": {
            "hard_filters": [
                F.col("soft_label") == "high_energy",
                (F.col("Tempo") >= 110) & (F.col("Tempo") <= 150),
            ],
            "targets": {
                "energy_proxy":   0.95,
                "tempo_proxy":    0.9,
                "loud_proxy":     0.9,
                "dance_proxy":    0.8,
                "acoustic_proxy": 0.2,
                "speech_proxy":   0.6,
                "live_proxy":     0.6,
                "valence_proxy":  0.5,
            },
            "weights": {
                "energy_proxy":   1.2,
                "tempo_proxy":    1.0,
                "loud_proxy":     1.0,
                "dance_proxy":    0.9,
                "speech_proxy":   0.6,
                "live_proxy":     0.5,
                "valence_proxy":  0.3,
                "acoustic_proxy": 0.2,
            },
        },

        # 18. I’m unbothered era (sassy)
        "unbothered_sassy": {
            "hard_filters": [
                (F.col("soft_label") == "high_energy"),
                (F.col("Tempo") >= 100) & (F.col("Tempo") <= 130),
                (F.col("Popularity") >= 40),
            ],
            "targets": {
                "energy_proxy":   0.9,
                "tempo_proxy":    0.8,
                "loud_proxy":     0.9,
                "dance_proxy":    0.9,
                "acoustic_proxy": 0.3,
                "speech_proxy":   0.7,
                "live_proxy":     0.7,
                "valence_proxy":  0.9,
            },
            "weights": {
                "energy_proxy":   1.0,
                "dance_proxy":    1.0,
                "valence_proxy":  0.9,
                "tempo_proxy":    0.8,
                "loud_proxy":     0.8,
                "speech_proxy":   0.7,
                "live_proxy":     0.6,
                "acoustic_proxy": 0.3,
            },
        },

        # 19. Slow morning
        "slow_morning": {
            "hard_filters": [
                (F.col("Tempo") >= 60) & (F.col("Tempo") <= 80),
                F.col("soft_label").isin("low_energy", "medium_energy"),
                (F.col("Popularity") >= 30),
            ],
            "targets": {
                "energy_proxy":   0.4,
                "tempo_proxy":    0.3,
                "loud_proxy":     0.3,
                "dance_proxy":    0.2,
                "acoustic_proxy": 0.8,
                "speech_proxy":   0.3,
                "live_proxy":     0.4,
                "valence_proxy":  0.4,
            },
            "weights": {
                "acoustic_proxy": 1.0,
                "energy_proxy":   0.8,
                "tempo_proxy":    0.8,
                "valence_proxy":  0.6,
                "loud_proxy":     0.5,
                "live_proxy":     0.4,
                "speech_proxy":   0.4,
                "dance_proxy":    0.3,
            },
        },

        # 20. Post breakup depressed
        "post_breakup_depressed": {
            "hard_filters": [
                F.col("soft_label") == "low_energy",
                (F.col("Tempo") >= 60) & (F.col("Tempo") <= 100),
            ],
            "targets": {
                "energy_proxy":   0.25,
                "tempo_proxy":    0.35,
                "loud_proxy":     0.3,
                "dance_proxy":    0.2,
                "acoustic_proxy": 0.7,
                "speech_proxy":   0.3,
                "live_proxy":     0.4,
                "valence_proxy":  0.2,
            },
            "weights": {
                "valence_proxy":  1.0,
                "energy_proxy":   0.9,
                "tempo_proxy":    0.7,
                "acoustic_proxy": 0.6,
                "dance_proxy":    0.4,
                "loud_proxy":     0.4,
                "speech_proxy":   0.3,
                "live_proxy":     0.3,
            },
        },

        # 21. Midnight existential
        "midnight_existential": {
            "hard_filters": [
                F.col("soft_label").isin("low_energy", "medium_energy"),
                (F.col("Tempo") >= 60) & (F.col("Tempo") <= 80),
            ],
            "targets": {
                "energy_proxy":   0.4,
                "tempo_proxy":    0.3,
                "loud_proxy":     0.3,
                "dance_proxy":    0.3,
                "acoustic_proxy": 0.6,
                "speech_proxy":   0.6,
                "live_proxy":     0.7,
                "valence_proxy":  0.5,
            },
            "weights": {
                "speech_proxy":   1.0,
                "live_proxy":     0.9,
                "energy_proxy":   0.7,
                "tempo_proxy":    0.7,
                "acoustic_proxy": 0.5,
                "dance_proxy":    0.5,
                "loud_proxy":     0.4,
                "valence_proxy":  0.4,
            },
        },

        # 22. Hot girl walk
        "hot_girl_walk": {
            "hard_filters": [
                (F.col("Tempo") >= 60) & (F.col("Tempo") <= 130),
                (F.col("Popularity") >= 40),
            ],
            "targets": {
                "energy_proxy":   0.6,
                "tempo_proxy":    0.6,
                "loud_proxy":     0.6,
                "dance_proxy":    0.7,
                "acoustic_proxy": 0.4,
                "speech_proxy":   0.5,
                "live_proxy":     0.6,
                "valence_proxy":  0.8,
            },
            "weights": {
                "valence_proxy":  1.0,
                "dance_proxy":    0.9,
                "tempo_proxy":    0.7,
                "energy_proxy":   0.7,
                "loud_proxy":     0.6,
                "live_proxy":     0.5,
                "speech_proxy":   0.4,
                "acoustic_proxy": 0.3,
            },
        },

        # 23. Main character energy
        "main_character_energy": {
            "hard_filters": [
                F.col("soft_label") == "high_energy",
                (F.col("Tempo") >= 70) & (F.col("Tempo") <= 100),
                (F.col("Popularity") >= 40),
            ],
            "targets": {
                "energy_proxy":   0.8,
                "tempo_proxy":    0.5,
                "loud_proxy":     0.7,
                "dance_proxy":    0.8,
                "acoustic_proxy": 0.4,
                "speech_proxy":   0.6,
                "live_proxy":     0.6,
                "valence_proxy":  0.9,
            },
            "weights": {
                "valence_proxy":  1.0,
                "dance_proxy":    1.0,
                "energy_proxy":   0.9,
                "tempo_proxy":    0.7,
                "loud_proxy":     0.7,
                "speech_proxy":   0.6,
                "live_proxy":     0.4,
                "acoustic_proxy": 0.3,
            },
        },

        # 24. Dream / escapist
        "dream_escapist": {
            "hard_filters": [
                (F.col("soft_label") == "low_energy"),
                (F.col("Tempo") >= 100) & (F.col("Tempo") <= 130),
                (F.col("is_exper") | F.col("is_rock") | F.col("is_indie")),
            ],
            "targets": {
                "energy_proxy":   0.3,
                "tempo_proxy":    0.7,
                "loud_proxy":     0.8,
                "dance_proxy":    0.3,
                "acoustic_proxy": 0.7,
                "speech_proxy":   0.2,
                "live_proxy":     0.4,
                "valence_proxy":  0.2,
            },
            "weights": {
                "valence_proxy":  1.0,
                "tempo_proxy":    0.8,
                "loud_proxy":     0.8,
                "acoustic_proxy": 0.7,
                "energy_proxy":   0.6,
                "dance_proxy":    0.4,
                "live_proxy":     0.4,
                "speech_proxy":   0.3,
            },
        },

        # 25. Going out
        "going_out": {
            "hard_filters": [
                F.col("soft_label").isin("medium_energy", "high_energy"),
                (F.col("Tempo") >= 80) & (F.col("Tempo") <= 130),
                (F.col("Popularity") >= 40),
            ],
            "targets": {
                "energy_proxy":   0.85,
                "tempo_proxy":    0.75,
                "loud_proxy":     0.9,
                "dance_proxy":    0.9,
                "acoustic_proxy": 0.5,
                "speech_proxy":   0.6,
                "live_proxy":     0.8,
                "valence_proxy":  0.9,
            },
            "weights": {
                "energy_proxy":   1.0,
                "dance_proxy":    1.0,
                "loud_proxy":     0.9,
                "valence_proxy":  0.9,
                "tempo_proxy":    0.8,
                "live_proxy":     0.7,
                "speech_proxy":   0.5,
                "acoustic_proxy": 0.3,
            },
        },
    }


    # In[19]:


    mood_rows = []
    for mood_name, cfg in MOOD_CONFIG.items():
        row_dict = {"mood": mood_name}
        for col in proxy_cols:
            # default target 0.5 if not specified
            row_dict[col] = float(cfg["targets"].get(col, 0.5))
        mood_rows.append(Row(**row_dict))

    mood_centroids = ss.createDataFrame(mood_rows)

    mood_assembler = VectorAssembler(inputCols=proxy_cols, outputCol="features")
    mood_centroids = mood_assembler.transform(mood_centroids)

    #mood_centroids.select("mood", "features").show(truncate=False)


    # In[20]:


    dot_udf = F.udf(dot, T.DoubleType())
    norm_udf = F.udf(norm, T.DoubleType())

    def _recommend_for_mood(df, mood_name, limit=50):
        cfg = MOOD_CONFIG[mood_name]

        base = df

        for cond in cfg["hard_filters"]:
            base = base.filter(cond)

        base = base.na.drop(subset=proxy_cols)

        score_expr = None
        for col_name, target in cfg["targets"].items():
            w = cfg["weights"].get(col_name, 1.0)
            term = w * (1.0 - F.abs(F.col(col_name) - F.lit(target)))
            score_expr = term if score_expr is None else (score_expr + term)

        base = base.withColumn("mood_score", score_expr)

        return (
            base.orderBy(F.col("mood_score").desc(), F.col("Popularity").desc())
            .limit(limit)
            .select(
                "track_id",
                "song",
                "Artist(s)",
                "Genre",
                "Popularity",
                "soft_label",
                "mood_score",
            )
        )

    recommend_for_mood = _recommend_for_mood

    return

def generate_playlist_for_user(user_track_ids, limit=10):
    """
    Input:
      - user_track_ids: list of track_id values from the uploaded CSV 
      - limit: how many recommendations to return

    Output:
      - predicted_mood: string (e.g. "villain_arc_gym")
      - predicted_score: float cosine similarity
      - recs_df: Spark DataFrame of recommended songs
    """
    _initialize_model()
    if not user_track_ids:
        raise ValueError("user_track_ids list is empty")

    user_songs = cleaned_df.filter(F.col("track_id").isin(user_track_ids))

    if user_songs.limit(1).count() == 0:
        raise ValueError("None of the provided track_ids exist in cleaned_df")

    user_profile = user_songs.agg(
        *[F.avg(c).alias(c) for c in proxy_cols]
    )

    user_profile_df = (
        mood_assembler
            .transform(user_profile)
            .select(F.col("features").alias("playlist_vec"))
            .withColumn("join_key", F.lit(1))
    )

    mood_df = (
        mood_centroids
            .select("mood", F.col("features").alias("mood_vec"))
            .withColumn("join_key", F.lit(1))
    )

    pairs = user_profile_df.join(mood_df, on="join_key")

    mood_scores = (
        pairs
          .withColumn("dot_product",   dot_udf("playlist_vec", "mood_vec"))
          .withColumn("norm_playlist", norm_udf("playlist_vec"))
          .withColumn("norm_mood",     norm_udf("mood_vec"))
          .withColumn(
              "cosine_similarity",
              F.when(
                  (F.col("norm_playlist") > 0) & (F.col("norm_mood") > 0),
                  F.col("dot_product") / (F.col("norm_playlist") * F.col("norm_mood"))
              ).otherwise(F.lit(None).cast("double"))
          )
          .select("mood", "cosine_similarity")
          .orderBy(F.col("cosine_similarity").desc())
    )

    top = mood_scores.first()
    if top is None or top["cosine_similarity"] is None:
        raise RuntimeError("Could not compute mood scores")

    predicted_mood = top["mood"]
    predicted_score = float(top["cosine_similarity"])
    recs_df = (
        recommend_for_mood(cleaned_df, predicted_mood, limit=limit)
          .filter(~F.col("track_id").isin(user_track_ids))
    )

    return predicted_mood, predicted_score, recs_df


# In[2]:


def generate_playlist_payload(user_track_ids, limit=10):
    _initialize_model()
    mood, score, recs_df = generate_playlist_for_user(user_track_ids, limit)
    tracks = [row.asDict() for row in recs_df.collect()]
    return {
        "predicted_mood": mood,
        "mood_score": score,
        "tracks": tracks,
    }


if __name__ == "__main__":
    sample_ids = [47142, 57726, 524853]
    mood, score, sample_recs = generate_playlist_for_user(sample_ids, limit=10)
    print(f"Sample mood: {mood} ({score:.4f})")
    sample_recs.show(truncate=False)
    ss.stop()
