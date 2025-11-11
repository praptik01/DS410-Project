#!/usr/bin/env python
# coding: utf-8

# # Import Libraries

# In[3]:


import pyspark
import pandas as pd
import numpy as np
import math


# In[5]:


from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType, FloatType
from pyspark.sql.functions import col, column
from pyspark.sql.functions import expr
from pyspark.sql.functions import split
from pyspark.sql import Row
from pyspark.sql import functions as F
from functools import reduce
from operator import add
from pyspark.ml.feature import VectorAssembler


# # Create SparkSessions and SparkContext

# In[101]:


ss=SparkSession.builder.master("local").appName("Spotify Playlist Generator").getOrCreate()


# In[103]:


ss.sparkContext.setCheckpointDir("~/scratch")


# # Read Data

# In[117]:


spotify_DF = ss.read.csv("./spotify_dataset.csv", header=True, inferSchema = True)


# In[119]:


train_df, validation_df, test_df = spotify_DF.randomSplit([0.8, 0.1, 0.1], seed=42)


# In[121]:


train_df.printSchema()


# In[123]:


sampled_df = train_df.sample(withReplacement=False, fraction=0.1, seed=67)


# In[125]:


spotify_DF.first()
# Expected CSV columns (minimum): title, artist, album, release_year, explicit, playlist_title
expected_cols = ['song', 'Artist(s)','Energy','Genre', 'Album', 'Tempo', 'Loudness (db)', 'Popularity', 'Liveness', 'Acousticness', 'Speechiness', 'Danceability', 'Positiveness', 'Time signature', 'Instrumentalness', 'Release Date','Explicit']
for col in expected_cols:
    if col not in spotify_DF.columns:
        raise ValueError(f"Missing expected column: {col}")


# # Clean & Normalize text columns 

# In[128]:


sampled_df.first()
# Expected CSV columns (minimum): title, artist, album, release_year, explicit, playlist_title
expected_cols = ['song', 'Artist(s)','Energy','Genre', 'Album', 'Tempo', 'Loudness (db)', 'Popularity', 'Liveness', 'Acousticness', 'Speechiness', 'Danceability', 'Positiveness', 'Time signature', 'Instrumentalness', 'Release Date','Explicit']
for col in expected_cols:
    if col not in sampled_df.columns:
        raise ValueError(f"Missing expected column: {col}")


# In[130]:


def norm(col):
    return F.lower(F.trim(F.regexp_replace(col, r"\s+", " ")))

df = (
    spotify_DF.withColumn("song_norm", norm(F.col("song")))
      .withColumn("artist_norm", norm(F.col("`Artist(s)`")))
      .withColumn("genre_norm", norm(F.col("Genre")))
      .withColumn("album_norm", norm(F.col("Album")))
)
df.select("song","song_norm","Artist(s)","artist_norm","Genre","genre_norm").show(10, truncate=False)


# # Add a unique ID & drop duplicates

# In[133]:


from pyspark.sql.functions import monotonically_increasing_id

clean_df = df
clean_df = df.withColumn("track_id", monotonically_increasing_id())
clean_df["track_id"]
before = clean_df.count()
clean_df = clean_df.dropDuplicates(["song_norm", "artist_norm"])
after = clean_df.count()


# In[134]:


print(before)
print(after)


# # Handle nulls & fix data types

# In[138]:


clean_df = clean_df.fillna({
    "genre_norm": "unknown",
    "artist_norm": "unknown",
    "album_norm": "unknown",
    "song_norm": "unknown"
})

clean_df = clean_df.fillna({
    "Energy": 0.0,
    "Tempo": 0.0,
    "Popularity": 0,
    "Liveness": 0.0,
    "Acousticness": 0.0,
    "Speechiness": 0.0,
    "Danceability": 0.0,
    "Positiveness": 0.0,
    "Instrumentalness": 0.0
})

cast_map = {
    "Energy": FloatType(),
    "Tempo": FloatType(),
    "Popularity": IntegerType(),
    "Liveness": FloatType(),
    "Acousticness": FloatType(),
    "Speechiness": FloatType(),
    "Danceability": FloatType(),
    "Positiveness": FloatType(),
    "Instrumentalness": FloatType()
}
for col_name, col_type in cast_map.items():
    clean_df = clean_df.withColumn(col_name, F.col(col_name).cast(col_type))
clean_df.printSchema()


# # Keyword flags + coarse genre

# In[39]:


from pyspark.sql import functions as F

def norm(col):
    return F.lower(F.trim(F.regexp_replace(col, r"\s+", " ")))

df = (
    spotify_DF
      .withColumn("song_norm",   norm(F.col("song")))
      .withColumn("artist_norm", norm(F.col("`Artist(s)`")))
      .withColumn("genre_norm",  norm(F.col("Genre")))
      .withColumn("album_norm",  norm(F.col("Album")))
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
    return F.not_(any_rx(col_or_str, pattern))

def between(colname, lo, hi):
    return (F.col(colname) >= F.lit(lo)) & (F.col(colname) <= F.lit(hi))

df = (df
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

df = df.withColumn(
    "genre_coarse",
    F.when(g_any_rx(r"(christmas|xmas|holiday|noel)"),              F.lit("holiday"))
     .when(g_any_rx(r"(disney|pixar)"),                             F.lit("disney"))
     .when(g_any_rx(r"(soundtrack|score)"),                         F.lit("soundtrack"))
     .when(g_any_rx(r"(lo[\s\-]?fi)"),                              F.lit("lofi"))
     .when(g_any_rx(r"(reggaeton|reggae|latin)"),                   F.lit("latin_reggae"))
     .when(g_any_rx(r"(christian|worship|gospel)"),                 F.lit("worship"))
     .when(g_any_rx(r"(classical)"),                                F.lit("classical"))
     .when(g_any_rx(r"(hip[\s\-]?hop|rap|trap|grime|cloud rap|emo rap)"),
                                                                F.lit("hiphop"))
     .when(g_any_rx(r"(electronic|electro|edm|house|techno|trance|synthpop|electropop|dance|drum and bass|dubstep|dub|chillwave|trip[\s\-]?hop|ambient|chillout)"),
                                                                F.lit("edm"))
     .when(g_any_rx(r"(metal|heavy metal|death metal|black metal|thrash metal|doom metal|progressive metal|power metal|metalcore|deathcore|hardcore|screamo)"),
                                                                F.lit("metal"))
     .when(g_any_rx(r"(rock|alternative rock|pop rock|hard rock|classic rock|garage rock|post[\s\-]?punk|punk( rock)?|grunge|britpop|new wave|math rock|shoegaze|psychedelic rock|progressive rock|post[\s\-]?hardcore)"),
                                                                F.lit("rock"))
     .when(g_any_rx(r"(indie|indie rock|indie pop|dream pop|alternative)"),   F.lit("indie"))
     .when(g_any_rx(r"(folk|alt[\s\-]?country|acoustic|country)"),            F.lit("folk"))
     .when(g_any_rx(r"(experimental|psychedelic)"),                           F.lit("experimental"))
     .when(g_any_rx(r"(jazz|soul|funk|blues)"),                               F.lit("jazz_soul"))
     .when(g_any_rx(r"(pop|k[\s\-]?pop|j[\s\-]?pop|dancehall)"),              F.lit("pop"))
     .otherwise(F.lit("pop"))
)

if "Loudness (db)" in df.columns and "Loudness" not in df.columns: #bc loudness column is loudness (db)
    df = df.withColumnRenamed("Loudness (db)", "Loudness")


# # Proxy features (0–1) from genre + keywords

# In[30]:


sampled_df.head()


# # Clean & Normalize text columns 

# In[13]:


def norm(col):
    return F.lower(F.trim(F.regexp_replace(col, r"\s+", " ")))

df = (
    sampled_df.withColumn("song_norm", norm(F.col("song")))
      .withColumn("artist_norm", norm(F.col("`Artist(s)`")))
      .withColumn("genre_norm", norm(F.col("Genre")))
      .withColumn("album_norm", norm(F.col("Album")))
)
df.select("song","song_norm","Artist(s)","artist_norm","Genre","genre_norm").show(10, truncate=False)


# # Add a unique ID & drop duplicates

# # Handle nulls & fix data types

# # Keyword flags + coarse genre

# In[14]:


from pyspark.sql import functions as F

def norm(col):
    return F.lower(F.trim(F.regexp_replace(col, r"\s+", " ")))

df = (
    sampled_df
      .withColumn("song_norm",   norm(F.col("song")))
      .withColumn("artist_norm", norm(F.col("`Artist(s)`")))
      .withColumn("genre_norm",  norm(F.col("Genre")))
      .withColumn("album_norm",  norm(F.col("Album")))
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
    return F.not_(any_rx(col_or_str, pattern))

def between(colname, lo, hi):
    return (F.col(colname) >= F.lit(lo)) & (F.col(colname) <= F.lit(hi))

df = (df
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

df = df.withColumn(
    "genre_coarse",
    F.when(g_any_rx(r"(christmas|xmas|holiday|noel)"),              F.lit("holiday"))
     .when(g_any_rx(r"(disney|pixar)"),                             F.lit("disney"))
     .when(g_any_rx(r"(soundtrack|score)"),                         F.lit("soundtrack"))
     .when(g_any_rx(r"(lo[\s\-]?fi)"),                              F.lit("lofi"))
     .when(g_any_rx(r"(reggaeton|reggae|latin)"),                   F.lit("latin_reggae"))
     .when(g_any_rx(r"(christian|worship|gospel)"),                 F.lit("worship"))
     .when(g_any_rx(r"(classical)"),                                F.lit("classical"))
     .when(g_any_rx(r"(hip[\s\-]?hop|rap|trap|grime|cloud rap|emo rap)"),
                                                                F.lit("hiphop"))
     .when(g_any_rx(r"(electronic|electro|edm|house|techno|trance|synthpop|electropop|dance|drum and bass|dubstep|dub|chillwave|trip[\s\-]?hop|ambient|chillout)"),
                                                                F.lit("edm"))
     .when(g_any_rx(r"(metal|heavy metal|death metal|black metal|thrash metal|doom metal|progressive metal|power metal|metalcore|deathcore|hardcore|screamo)"),
                                                                F.lit("metal"))
     .when(g_any_rx(r"(rock|alternative rock|pop rock|hard rock|classic rock|garage rock|post[\s\-]?punk|punk( rock)?|grunge|britpop|new wave|math rock|shoegaze|psychedelic rock|progressive rock|post[\s\-]?hardcore)"),
                                                                F.lit("rock"))
     .when(g_any_rx(r"(indie|indie rock|indie pop|dream pop|alternative)"),   F.lit("indie"))
     .when(g_any_rx(r"(folk|alt[\s\-]?country|acoustic|country)"),            F.lit("folk"))
     .when(g_any_rx(r"(experimental|psychedelic)"),                           F.lit("experimental"))
     .when(g_any_rx(r"(jazz|soul|funk|blues)"),                               F.lit("jazz_soul"))
     .when(g_any_rx(r"(pop|k[\s\-]?pop|j[\s\-]?pop|dancehall)"),              F.lit("pop"))
     .otherwise(F.lit("pop"))
)

if "Loudness (db)" in df.columns and "Loudness" not in df.columns: #bc loudness column is loudness (db)
    df = df.withColumnRenamed("Loudness (db)", "Loudness")


# # Proxy features (0–1) from genre + keywords

# In[15]:


BASE = {
    "energy": 0.50, "dance": 0.50, "loud": 0.50, "tempo": 0.50,
    "acoustic": 0.30, "speech": 0.30, "live": 0.10, "valence": 0.50,
}

def lit(x): return F.lit(float(x))
def clamp01(c): return F.greatest(lit(0.0), F.least(lit(1.0), c))

gb = F.col("genre_coarse")
df = (df
    .withColumn("energy_proxy",   lit(BASE["energy"]))
    .withColumn("dance_proxy",    lit(BASE["dance"]))
    .withColumn("loud_proxy",     lit(BASE["loud"]))
    .withColumn("tempo_proxy",    lit(BASE["tempo"]))
    .withColumn("acoustic_proxy", lit(BASE["acoustic"]))
    .withColumn("speech_proxy",   lit(BASE["speech"]))
    .withColumn("live_proxy",     lit(BASE["live"]))
    .withColumn("valence_proxy",  lit(BASE["valence"]))
)

def bump(colname, inc, cond):
    return F.when(cond, F.col(colname) + lit(inc)).otherwise(F.col(colname))

#edm
for c, inc in [("energy_proxy",0.70),("dance_proxy",0.70),("loud_proxy",0.70),("tempo_proxy",0.65)]:
    df = df.withColumn(c, bump(c, inc, gb == "edm"))

#hiphop
for c, inc in [("energy_proxy",0.30),("dance_proxy",0.30),("loud_proxy",0.30),("tempo_proxy",0.20)]:
    df = df.withColumn(c, bump(c, inc, gb == "hiphop"))
df = df.withColumn("speech_proxy", bump("speech_proxy", 0.15, gb == "hiphop"))

#rock
for c, inc in [("energy_proxy",0.40),("loud_proxy",0.40),("tempo_proxy",0.35),("dance_proxy",0.20)]:
    df = df.withColumn(c, bump(c, inc, gb == "rock"))

#metal
for c, inc in [("energy_proxy",0.50),("loud_proxy",0.60),("tempo_proxy",0.30)]:
    df = df.withColumn(c, bump(c, inc, gb == "metal"))

#indie/folk
for g in ["indie","folk"]:
    df = df.withColumn("energy_proxy",   bump("energy_proxy",  -0.10, gb == g))
    df = df.withColumn("acoustic_proxy", bump("acoustic_proxy",  0.40, gb == g))
    df = df.withColumn("valence_proxy",  bump("valence_proxy",  -0.05, gb == g))

#lofi
df = df.withColumn("energy_proxy",   bump("energy_proxy",  -0.30, gb == "lofi"))
df = df.withColumn("dance_proxy",    bump("dance_proxy",    0.10, gb == "lofi"))
df = df.withColumn("acoustic_proxy", bump("acoustic_proxy", 0.20, gb == "lofi"))
df = df.withColumn("live_proxy",     bump("live_proxy",     0.05, gb == "lofi"))

#soundtrack
df = df.withColumn("live_proxy",     bump("live_proxy",     0.20, gb == "soundtrack"))
df = df.withColumn("speech_proxy",   bump("speech_proxy",   0.10, gb == "soundtrack"))
df = df.withColumn("energy_proxy",   bump("energy_proxy",  -0.05, gb == "soundtrack"))

#disney/holiday
for g in ["disney","holiday"]:
    df = df.withColumn("valence_proxy",  bump("valence_proxy",  0.20, gb == g))
    df = df.withColumn("live_proxy",     bump("live_proxy",     0.10, gb == g))

#pop
for c, inc in [("energy_proxy",0.30),("dance_proxy",0.30),("loud_proxy",0.10),("tempo_proxy",0.30)]:
    df = df.withColumn(c, bump(c, inc, gb == "pop"))

#classical
for c, inc in [("energy_proxy",0.10),("dance_proxy",0.10),("loud_proxy",0.10),("tempo_proxy",0.20)]:
    df = df.withColumn(c, bump(c, inc, gb == "classical"))

#latin/reggae
for c, inc in [("energy_proxy",0.40),("dance_proxy",0.50),("loud_proxy",0.30),("tempo_proxy",0.50)]:
    df = df.withColumn(c, bump(c, inc, gb == "latin_reggae"))

#jazzsoul
for c, inc in [("energy_proxy",-0.10),("dance_proxy",0.10),("loud_proxy",0.10),("tempo_proxy",0.10)]:
    df = df.withColumn(c, bump(c, inc, gb == "jazz_soul"))
df = df.withColumn("acoustic_proxy", bump("acoustic_proxy", 0.40, gb == "jazz_soul"))

#worship
for c, inc in [("energy_proxy",-0.10),("dance_proxy",0.10),("loud_proxy",0.10),("tempo_proxy",0.10)]:
    df = df.withColumn(c, bump(c, inc, gb == "worship"))
df = df.withColumn("acoustic_proxy", bump("acoustic_proxy", 0.20, gb == "worship"))

#experimental
for c, inc in [("energy_proxy",0.10),("dance_proxy",0.10),("loud_proxy",0.10),("tempo_proxy",0.20)]:
    df = df.withColumn(c, bump(c, inc, gb == "experimental"))

df = (df
    #remix or club
    .withColumn("energy_proxy", bump("energy_proxy", 0.10, F.col("kw_remix")))
    .withColumn("dance_proxy",  bump("dance_proxy",  0.10, F.col("kw_remix")))
    .withColumn("tempo_proxy",  bump("tempo_proxy",  0.10, F.col("kw_remix")))

    #acoustic
    .withColumn("energy_proxy",   bump("energy_proxy",   -0.20, F.col("kw_acoustic")))
    .withColumn("acoustic_proxy", bump("acoustic_proxy",  0.30,  F.col("kw_acoustic")))
    .withColumn("live_proxy",     bump("live_proxy",      0.05,  F.col("kw_acoustic")))

    #live or energetic
    .withColumn("energy_proxy",   bump("energy_proxy",   -0.10, F.col("kw_live")))
    .withColumn("acoustic_proxy", bump("acoustic_proxy",  0.20,  F.col("kw_live")))
    .withColumn("live_proxy",     bump("live_proxy",      0.20,  F.col("kw_live")))

    #ballad
    .withColumn("energy_proxy",  bump("energy_proxy",  -0.10, F.col("kw_ballad")))
    .withColumn("valence_proxy", bump("valence_proxy", -0.10, F.col("kw_ballad")))

    #instrumental or lofi
    .withColumn("speech_proxy",  bump("speech_proxy",  -0.20, F.col("kw_instrumental")))
)

df = df.withColumn(
    "explicit_bool",
    F.when(F.lower(F.coalesce(F.col("Explicit"), F.lit(""))).isin("true","1","yes"), F.lit(True))
     .otherwise(F.lit(False))
)

df = (df
    .withColumn("energy_proxy", bump("energy_proxy",  0.05, F.col("explicit_bool")))
    .withColumn("speech_proxy", bump("speech_proxy",  0.10, F.col("explicit_bool")))
)

for c in ["energy_proxy","dance_proxy","loud_proxy","tempo_proxy",
          "acoustic_proxy","speech_proxy","live_proxy","valence_proxy"]:
    df = df.withColumn(c, clamp01(F.col(c)))

df.select("song","genre_coarse","kw_remix","kw_acoustic","kw_live","kw_ballad",
          "kw_instrumental","energy_proxy","dance_proxy","loud_proxy","tempo_proxy",
          "acoustic_proxy","speech_proxy","live_proxy","valence_proxy").show(12, truncate=False)


# # Hard labels (priority)

# In[ ]:


POP_IS_0_100 = True   # if popularity already 0-1, set False
POP_HIGH  = 70.0 if POP_IS_0_100 else 0.70
POP_MED   = 50.0 if POP_IS_0_100 else 0.50

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
    .otherwise(F.lit(None))


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
)


# # Soft label (simple scorer UDF, pick max)

# In[ ]:


df = (df
 .withColumn("energy",       bump("energy",       0.70, gb=="edm"))
 .withColumn("danceability", bump("danceability", 0.70, gb=="edm"))
 .withColumn("loudness",     bump("loudness",     0.70, gb=="edm"))
 .withColumn("tempo",        bump("tempo",        0.65, gb=="edm"))

 # rnb/hiphop: +0.10 energy, +0.10 dance, +0.15 speech, +0.05 loud
 .withColumn("energy",       bump("energy",       0.10, gb=="hiphop"))
 .withColumn("danceability", bump("danceability", 0.10, gb=="hiphop"))
 .withColumn("speechiness",  bump("speechiness",  0.15, gb=="hiphop"))
 .withColumn("loudness",     bump("loudness",     0.05, gb=="hiphop"))
 # rock: +0.40 energy, +0.40 loud, +0.35 tempo, +0.20 dance
 .withColumn("energy",       bump("energy",       0.40, gb=="rock"))
 .withColumn("loudness",     bump("loudness",     0.40, gb=="rock"))
 .withColumn("tempo",        bump("tempo",        0.35, gb=="rock"))
 .withColumn("danceability", bump("danceability", 0.20, gb=="rock"))

 # indie/folk: −0.10 energy, +0.40 acoustic, −0.05 valence
 .withColumn("energy",       bump("energy",      -0.10, gb=="indie_folk"))
 .withColumn("acousticness", bump("acousticness", 0.40, gb=="indie_folk"))
 .withColumn("valence",      bump("valence",     -0.05, gb=="indie_folk"))

# lofi/jazz: −0.30 energy, +0.10 dance, +0.20 acoustic, +0.05 live
 .withColumn("energy",       bump("energy",      -0.30, gb=="lofi_jazz"))
 .withColumn("danceability", bump("danceability", 0.10, gb=="lofi_jazz"))
 .withColumn("acousticness", bump("acousticness", 0.20, gb=="lofi_jazz"))
 .withColumn("liveness",     bump("liveness",     0.05, gb=="lofi_jazz"))

 # soundtrack: +0.20 live, +0.10 speech, −0.05 energy
 .withColumn("liveness",     bump("liveness",     0.20, gb=="soundtrack"))
 .withColumn("speechiness",  bump("speechiness",  0.10, gb=="soundtrack"))
 .withColumn("energy",       bump("energy",      -0.05, gb=="soundtrack"))

 # disney/holiday: +0.20 valence, +0.10 live
 .withColumn("valence",      bump("valence",      0.20, gb=="disney_holiday"))
 .withColumn("liveness",     bump("liveness",     0.10, gb=="disney_holiday"))
# pop: +0.30 energy, +0.30 dance, +0.10 loud, +0.30 tempo
 .withColumn("energy",       bump("energy",       0.30, gb=="pop"))
 .withColumn("danceability", bump("danceability", 0.30, gb=="pop"))
 .withColumn("loudness",     bump("loudness",     0.10, gb=="pop"))
 .withColumn("tempo",        bump("tempo",        0.30, gb=="pop"))
 # classical: +0.10 energy, +0.10 dance, +0.10 loud, +0.20 tempo
 .withColumn("energy",       bump("energy",       0.10, gb=="classical"))
 .withColumn("danceability", bump("danceability", 0.10, gb=="classical"))
 .withColumn("loudness",     bump("loudness",     0.10, gb=="classical"))
 .withColumn("tempo",        bump("tempo",        0.20, gb=="classical"))

 # latin/reggae: +0.40 energy, +0.50 dance, +0.30 loud, +0.50 tempo
 .withColumn("energy",       bump("energy",       0.40, gb=="latin_reggae"))
 .withColumn("danceability", bump("danceability", 0.50, gb=="latin_reggae"))
 .withColumn("loudness",     bump("loudness",     0.30, gb=="latin_reggae"))
 .withColumn("tempo",        bump("tempo",        0.50, gb=="latin_reggae"))

# spiritual/worship: -0.10 energy, +0.10 dance, +0.10 loud, +0.10 tempo, +0.20 acoustic
 .withColumn("energy",       bump("energy",      -0.10, gb=="spiritual"))
 .withColumn("danceability", bump("danceability", 0.10, gb=="spiritual"))
 .withColumn("loudness",     bump("loudness",     0.10, gb=="spiritual"))
 .withColumn("tempo",        bump("tempo",        0.10, gb=="spiritual"))
 .withColumn("acousticness", bump("acousticness", 0.20, gb=="spiritual"))

 # jazz/soul: -0.10 energy, +0.10 dance, +0.10 loud, +0.10 tempo, +0.40 acoustic
 .withColumn("energy",       bump("energy",      -0.10, gb=="jazz_soul"))
 .withColumn("danceability", bump("danceability", 0.10, gb=="jazz_soul"))
 .withColumn("loudness",     bump("loudness",     0.10, gb=="jazz_soul"))
 .withColumn("tempo",        bump("tempo",        0.10, gb=="jazz_soul"))
 .withColumn("acousticness", bump("acousticness", 0.40, gb=="jazz_soul"))
)


# # Assign categories to existing songs based on label rules

# In[ ]:


df = df.withColumn(
    "assigned_category",
    F.when(F.col("hard_label").isNotNull(), F.col("hard_label"))
     .when(F.col("soft_score") >= F.lit(0.25), F.col("soft_label"))
)
df_agg = (
    df.groupBy("assigned_category")
      .agg(F.count("*").alias("song_count"))
)
total = df.count()
df_agg = df_agg.withColumn("percent", (F.col("song_count") / total) * 100)

df_agg.orderBy(F.desc("song_count")).show(50, truncate=False)


# In[ ]:


feature_cols = [
    "Energy", "Tempo", "Loudness", "Danceability", "Positiveness",
    "Acousticness", "Speechiness", "Liveness", "Instrumentalness"
]

cat_avgs = ( df.groupBy("assigned_category")
      .agg(*[F.avg(c).alias(c) for c in feature_cols])
)

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
df_vectorize = assembler.transform(cat_avgs)
df_vectorize.show(truncate=False)

a = df_vectorize.select("assigned_category", "features").alias("a")
b = df_vectorize.select("assigned_category", "features").alias("b")
pairs = (
    a.crossJoin(b)
     .select(
        F.col("a.assigned_category").alias("cat_i"),
        F.col("b.assigned_category").alias("cat_j"),
        F.col("a.features").alias("vector1"),
        F.col("b.features").alias("vector2"),
     )
)

def dot(v1, v2):
    return float(v1.dot(v2))

def norm(vec): 
    return float(vec.norm(2))

dot_udf = F.udf(dot,F.DoubleType())
norm_udf = F.udf(norm, F.DoubleType())

cosine_similarity = (
    pairs
      .withColumn("dot_product", dot_udf(F.col("vector1"), F.col("vector2")))
      .withColumn("norm_1",      norm_udf(F.col("vector1")))
      .withColumn("norm_2",      norm_udf(F.col("vector2")))
      .withColumn("cosine_similarity",
                  F.col("dot_product") / (F.col("norm_1") * F.col("norm_2")))
)
cosine_similarity.show()p



# In[ ]:


ss.stop()

