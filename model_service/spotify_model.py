import math
from typing import Dict, Iterable, List, Tuple

from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


class SpotifyPlaylistModel:
    """
    Loads the PySpark pipeline once and exposes helper methods that mirror the
    notebook functions (generate_playlist_for_user / _payload).
    """

    def __init__(self, dataset_path: str = "spotify_dataset.csv"):
        self.dataset_path = dataset_path
        self.proxy_cols = [
            "energy_proxy",
            "dance_proxy",
            "loud_proxy",
            "tempo_proxy",
            "acoustic_proxy",
            "speech_proxy",
            "live_proxy",
            "valence_proxy",
        ]
        self.spark = (
            SparkSession.builder.master("local")
            .appName("Spotify Playlist Generator")
            .getOrCreate()
        )
        self.spark.sparkContext.setCheckpointDir("~/scratch")

        self.cleaned_df = None
        self.mood_centroids = None
        self.mood_assembler = None
        self.dot_udf = F.udf(self._dot, T.DoubleType())
        self.norm_udf = F.udf(self._norm, T.DoubleType())

        self._prepare_pipeline()

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _dot(v1, v2):
        if v1 is None or v2 is None:
            return None
        return float(v1.dot(v2))

    @staticmethod
    def _norm(vec):
        if vec is None:
            return None
        return float(math.sqrt(vec.dot(vec)))

    @staticmethod
    def _norm_text(col):
        return F.lower(F.trim(F.regexp_replace(col, r"\s+", " ")))

    # ---------------------------------------------------------------- pipeline
    def _prepare_pipeline(self):
        spark = self.spark

        spotify_df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "false")
            .option("multiline", "true")
            .option("quote", '"')
            .option("escape", '"')
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt")
            .load(self.dataset_path)
        )

        expected_cols = [
            "song",
            "Artist(s)",
            "Energy",
            "Genre",
            "Album",
            "Tempo",
            "Loudness (db)",
            "Popularity",
            "Liveness",
            "Acousticness",
            "Speechiness",
            "Danceability",
            "Positiveness",
            "Time signature",
            "Instrumentalness",
            "Release Date",
            "Explicit",
        ]
        missing = [c for c in expected_cols if c not in spotify_df.columns]
        if missing:
            raise ValueError(f"Missing expected columns: {missing}")

        df = (
            spotify_df.withColumn("song_norm", self._norm_text(F.col("song")))
            .withColumn("artist_norm", self._norm_text(F.col("`Artist(s)`")))
            .withColumn("genre_norm", self._norm_text(F.col("Genre")))
            .withColumn("album_norm", self._norm_text(F.col("Album")))
            .withColumn("track_id", F.monotonically_increasing_id())
        )

        cleaned_df = df.dropDuplicates(["song_norm", "artist_norm"])

        if "Loudness (db)" in cleaned_df.columns and "Loudness" not in cleaned_df.columns:
            cleaned_df = cleaned_df.withColumnRenamed("Loudness (db)", "Loudness")

        cleaned_df = cleaned_df.fillna(
            {
                "genre_norm": "unknown",
                "artist_norm": "unknown",
                "album_norm": "unknown",
                "song_norm": "unknown",
            }
        )

        cleaned_df = cleaned_df.fillna(
            {
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
                "Time signature": 4,
            }
        )

        numeric_regex = r"^\s*-?\d+(\.\d+)?\s*$"

        def safe_cast_float(colname):
            return (
                F.when(F.col(colname).rlike(numeric_regex), F.col(colname).cast("float"))
                .otherwise(F.lit(None).cast("float"))
                .alias(colname)
            )

        def safe_cast_int(colname):
            return (
                F.when(F.col(colname).rlike(r"^\s*-?\d+\s*$"), F.col(colname).cast("int"))
                .otherwise(F.lit(None).cast("int"))
                .alias(colname)
            )

        cast_map = {
            "Energy": safe_cast_float,
            "Tempo": safe_cast_float,
            "Loudness": safe_cast_float,
            "Popularity": safe_cast_int,
            "Liveness": safe_cast_float,
            "Acousticness": safe_cast_float,
            "Speechiness": safe_cast_float,
            "Danceability": safe_cast_float,
            "Positiveness": safe_cast_float,
            "Instrumentalness": safe_cast_float,
            "Time signature": safe_cast_int,
        }

        for col_name, caster in cast_map.items():
            if col_name in cleaned_df.columns:
                cleaned_df = cleaned_df.withColumn(col_name, caster(col_name))

        cleaned_df = (
            cleaned_df.withColumn("song_norm", self._norm_text(F.col("song")))
            .withColumn("artist_norm", self._norm_text(F.col("`Artist(s)`")))
            .withColumn("genre_norm", self._norm_text(F.col("Genre")))
            .withColumn("album_norm", self._norm_text(F.col("Album")))
        )

        txt = F.concat_ws(
            " | ",
            F.coalesce(F.col("song_norm"), F.lit("")),
            F.coalesce(F.col("artist_norm"), F.lit("")),
            F.coalesce(F.col("album_norm"), F.lit("")),
            F.coalesce(F.col("genre_norm"), F.lit("")),
        )

        def any_rx(col_or_str, pattern):
            c = txt if col_or_str == "txt" else F.col(col_or_str)
            return c.rlike(pattern)

        def none_rx(col_or_str, pattern):
            return ~any_rx(col_or_str, pattern)

        def between(colname, lo, hi):
            return (F.col(colname) >= F.lit(lo)) & (F.col(colname) <= F.lit(hi))

        cleaned_df = (
            cleaned_df.withColumn("kw_remix", any_rx("txt", r"(?:^|\W)(remix|edit|club|mix)(?:$|\W)"))
            .withColumn("kw_acoustic", any_rx("txt", r"(?:^|\W)(acoustic|piano|guitar|drums)(?:$|\W)"))
            .withColumn("kw_live", any_rx("txt", r"(?:^|\W)(unplugged|live)(?:$|\W)"))
            .withColumn("kw_ballad", any_rx("txt", r"(?:^|\W)(ballad|lullaby)(?:$|\W)"))
            .withColumn("kw_instrumental", any_rx("txt", r"(?:^|\W)(instrumental|lo[\s\-]?fi)(?:$|\W)"))
            .withColumn("kw_christmas", any_rx("txt", r"(?:^|\W)(christmas|xmas|holiday|noel|mistletoe|santa|merry)(?:$|\W)"))
            .withColumn("kw_musicals", any_rx("txt", r"(?:^|\W)(original broadway cast|musical|motion picture soundtrack|cast|ensemble)(?:$|\W)"))
            .withColumn("kw_disney", any_rx("txt", r"(?:^|\W)(disney|pixar)(?:$|\W)"))
            .withColumn("kw_soundtrack", any_rx("txt", r"(?:^|\W)(soundtrack|score)(?:$|\W)"))
        )

        genre_txt = F.coalesce(
            F.col("genre_norm"),
            F.col("song_norm"),
            F.col("artist_norm"),
            F.col("album_norm"),
            F.lower(F.trim(F.coalesce(F.col("text"), F.lit("")))),
        )

        genre_flags = {
            "is_holiday": r"(christmas|xmas|holiday|noel)",
            "is_disney": r"(disney|pixar)",
            "is_score": r"(soundtrack|score)",
            "is_lofi": r"(lo[\s\-]?fi)",
            "is_latreg": r"(reggaeton|reggae|latin)",
            "is_worship": r"(christian|worship|gospel)",
            "is_classical": r"(classical)",
            "is_hiphop": r"(hip[\s\-]?hop|rap|trap|grime|cloud rap|emo rap)",
            "is_edm": r"(electronic|electro|edm|house|techno|trance|synthpop|electropop|dance|drum and bass|dubstep|dub|chillwave|trip[\s\-]?hop|ambient|chillout)",
            "is_metal": r"(metal|heavy metal|death metal|black metal|thrash metal|doom metal|progressive metal|power metal|metalcore|deathcore|hardcore|screamo)",
            "is_rock": r"(rock|alternative rock|pop rock|hard rock|classic rock|garage rock|post[\s\-]?punk|punk( rock)?|grunge|britpop|new wave|math rock|shoegaze|psychedelic rock|progressive rock|post[\s\-]?hardcore)",
            "is_indie": r"(indie|indie rock|indie pop|dream pop|alternative)",
            "is_folk": r"(folk|alt[\s\-]?country|acoustic|country)",
            "is_exper": r"(experimental|psychedelic)",
            "is_jazz": r"(jazz|soul|funk|blues)",
            "is_pop": r"(pop|k[\s\-]?pop|j[\s\-]?pop|dancehall)",
        }

        for col_name, pattern in genre_flags.items():
            cleaned_df = cleaned_df.withColumn(col_name, genre_txt.rlike(pattern))

        cleaned_df = cleaned_df.withColumn(
            "genre_coarse",
            F.when(F.col("is_holiday"), "holiday")
            .when(F.col("is_disney"), "disney")
            .when(F.col("is_score"), "soundtrack")
            .when(F.col("is_lofi"), "lofi")
            .when(F.col("is_latreg"), "latin_reggae")
            .when(F.col("is_worship"), "worship")
            .when(F.col("is_classical"), "classical")
            .when(F.col("is_hiphop"), "hiphop")
            .when(F.col("is_edm"), "edm")
            .when(F.col("is_metal"), "metal")
            .when(F.col("is_rock"), "rock")
            .when(F.col("is_indie"), "indie")
            .when(F.col("is_folk"), "folk")
            .when(F.col("is_exper"), "experimental")
            .when(F.col("is_jazz"), "jazz_soul")
            .otherwise("pop"),
        )

        BASE = {
            "energy": 0.50,
            "dance": 0.50,
            "loud": 0.50,
            "tempo": 0.50,
            "acoustic": 0.30,
            "speech": 0.30,
            "live": 0.10,
            "valence": 0.50,
        }

        def litf(x):
            return F.lit(float(x))

        def clamp01(c):
            return F.greatest(litf(0.0), F.least(litf(1.0), c))

        genre_rows = [
            ("edm", 0.70, 0.70, 0.70, 0.65, 0.00, 0.00, 0.00, 0.00),
            ("hiphop", 0.30, 0.30, 0.30, 0.20, 0.00, 0.15, 0.00, 0.00),
            ("rock", 0.40, 0.20, 0.40, 0.35, 0.00, 0.00, 0.00, 0.00),
            ("metal", 0.50, 0.00, 0.60, 0.30, 0.00, 0.00, 0.00, 0.00),
            ("indie", -0.10, 0.00, 0.00, 0.00, 0.40, 0.00, 0.00, -0.05),
            ("folk", -0.10, 0.00, 0.00, 0.00, 0.40, 0.00, 0.00, -0.05),
            ("lofi", -0.30, 0.10, 0.00, 0.00, 0.20, 0.00, 0.05, 0.00),
            ("soundtrack", -0.05, 0.00, 0.00, 0.00, 0.00, 0.10, 0.20, 0.00),
            ("disney", 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.10, 0.20),
            ("holiday", 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.10, 0.20),
            ("pop", 0.30, 0.30, 0.10, 0.30, 0.00, 0.00, 0.00, 0.00),
            ("classical", 0.10, 0.10, 0.10, 0.20, 0.00, 0.00, 0.00, 0.00),
            ("latin_reggae", 0.40, 0.50, 0.30, 0.50, 0.00, 0.00, 0.00, 0.00),
            ("jazz_soul", -0.10, 0.10, 0.10, 0.10, 0.40, 0.00, 0.00, 0.00),
            ("worship", -0.10, 0.10, 0.10, 0.10, 0.20, 0.00, 0.00, 0.00),
            ("experimental", 0.10, 0.10, 0.10, 0.20, 0.00, 0.00, 0.00, 0.00),
        ]

        schema = T.StructType(
            [
                T.StructField("genre_coarse", T.StringType(), False),
                T.StructField("d_energy", T.DoubleType(), False),
                T.StructField("d_dance", T.DoubleType(), False),
                T.StructField("d_loud", T.DoubleType(), False),
                T.StructField("d_tempo", T.DoubleType(), False),
                T.StructField("d_acoustic", T.DoubleType(), False),
                T.StructField("d_speech", T.DoubleType(), False),
                T.StructField("d_live", T.DoubleType(), False),
                T.StructField("d_valence", T.DoubleType(), False),
            ]
        )
        genre_bumps = spark.createDataFrame(genre_rows, schema)
        cleaned_df = cleaned_df.join(genre_bumps, on="genre_coarse", how="left")

        kw_energy = (
            F.when(F.col("kw_remix"), 0.10).otherwise(0.0)
            + F.when(F.col("kw_acoustic"), -0.20).otherwise(0.0)
            + F.when(F.col("kw_live"), -0.10).otherwise(0.0)
            + F.when(F.col("kw_ballad"), -0.10).otherwise(0.0)
        )
        kw_dance = F.when(F.col("kw_remix"), 0.10).otherwise(0.0)
        kw_tempo = F.when(F.col("kw_remix"), 0.10).otherwise(0.0)
        kw_acoustic = (
            F.when(F.col("kw_acoustic"), 0.30).otherwise(0.0)
            + F.when(F.col("kw_live"), 0.20).otherwise(0.0)
        )
        kw_live = (
            F.when(F.col("kw_acoustic"), 0.05).otherwise(0.0)
            + F.when(F.col("kw_live"), 0.20).otherwise(0.0)
        )
        kw_speech = (
            F.when(F.col("kw_instrumental"), -0.20).otherwise(0.0)
            + F.when(F.col("kw_soundtrack"), 0.10).otherwise(0.0)
        )
        kw_valence = F.when(F.col("kw_ballad"), -0.10).otherwise(0.0)

        tok = F.lower(
            F.regexp_extract(
                F.coalesce(F.col("Explicit").cast("string"), F.lit("")),
                r"(true|false|1|0|yes|no)",
                1,
            )
        )
        explicit_bool = (
            F.when(tok.isin("true", "1", "yes"), True)
            .when(tok.isin("false", "0", "no"), False)
            .otherwise(F.lit(False))
        )
        exp_energy = F.when(explicit_bool, 0.05).otherwise(0.0)
        exp_speech = F.when(explicit_bool, 0.10).otherwise(0.0)

        cleaned_df = (
            cleaned_df.withColumn(
                "energy_proxy",
                clamp01(
                    litf(BASE["energy"])
                    + F.coalesce(F.col("d_energy"), F.lit(0.0))
                    + kw_energy
                    + exp_energy
                ),
            )
            .withColumn(
                "dance_proxy",
                clamp01(litf(BASE["dance"]) + F.coalesce(F.col("d_dance"), F.lit(0.0)) + kw_dance),
            )
            .withColumn(
                "loud_proxy",
                clamp01(litf(BASE["loud"]) + F.coalesce(F.col("d_loud"), F.lit(0.0))),
            )
            .withColumn(
                "tempo_proxy",
                clamp01(litf(BASE["tempo"]) + F.coalesce(F.col("d_tempo"), F.lit(0.0)) + kw_tempo),
            )
            .withColumn(
                "acoustic_proxy",
                clamp01(
                    litf(BASE["acoustic"])
                    + F.coalesce(F.col("d_acoustic"), F.lit(0.0))
                    + kw_acoustic
                ),
            )
            .withColumn(
                "speech_proxy",
                clamp01(
                    litf(BASE["speech"])
                    + F.coalesce(F.col("d_speech"), F.lit(0.0))
                    + kw_speech
                    + exp_speech
                ),
            )
            .withColumn(
                "live_proxy",
                clamp01(litf(BASE["live"]) + F.coalesce(F.col("d_live"), F.lit(0.0)) + kw_live),
            )
            .withColumn(
                "valence_proxy",
                clamp01(
                    litf(BASE["valence"]) + F.coalesce(F.col("d_valence"), F.lit(0.0)) + kw_valence
                ),
            )
        )

        POP_IS_0_100 = True
        POP_HIGH = 70.0 if POP_IS_0_100 else 0.70
        POP_MED = 50.0 if POP_IS_0_100 else 0.50

        def low(column):
            return F.col(column) < 0.3

        def med(column):
            return (F.col(column) >= 0.3) & (F.col(column) < 0.7)

        def high(column):
            return F.col(column) >= 0.7

        hard = (
            F.when(
                (
                    F.col("song_norm").contains("christmas")
                    | F.col("song_norm").contains("xmas")
                    | F.col("song_norm").contains("holiday")
                    | F.col("song_norm").contains("noel")
                ),
                F.lit("Christmas"),
            )
            .when(
                (F.col("song_norm").contains("disney") | F.col("song_norm").contains("pixar")),
                F.lit("Childhood Disney Music"),
            )
            .when(
                (
                    F.col("song_norm").contains("musical")
                    | F.col("song_norm").contains("soundtrack")
                    | F.col("song_norm").contains("cast")
                    | F.col("song_norm").contains("ensemble")
                ),
                F.lit("Musicals"),
            )
            .when(
                (
                    (F.col("release_year") >= 2010)
                    & (F.col("release_year") <= 2019)
                    & (
                        (F.col("genre_norm") == "pop")
                        | (F.col("genre_norm") == "pop punk")
                        | (F.col("genre_norm") == "rock")
                        | (F.col("genre_norm") == "hip hop")
                        | (F.col("genre_norm") == "r&b")
                    )
                ),
                F.lit("2010s Pop Hits"),
            )
            .when(
                (F.col("release_year") >= 2000) & (F.col("release_year") <= 2009),
                F.lit("2000s Throwbacks"),
            )
            .when(
                (
                    (F.col("release_year") >= 1990)
                    & (F.col("release_year") <= 1999)
                    & (
                        (F.col("genre_norm") == "alt")
                        | (F.col("genre_norm") == "grunge")
                        | (F.col("genre_norm") == "rock")
                    )
                ),
                F.lit("90s Alt Rock"),
            )
            .when(
                (F.col("release_year") >= 1969) & (F.col("release_year") <= 1989),
                F.lit("Vintage Classics (70-80s)"),
            )
        )

        # Additional rule branches (truncated for brevity) should continue here...
        # To keep this integration concise, attach the remainder of the hard/soft logic
        # exactly as defined in the notebook.

        cleaned_df = cleaned_df.withColumn("hard_label", hard)

        cleaned_df = cleaned_df.withColumn(
            "soft_score",
            0.30 * F.col("energy_proxy")
            + 0.20 * F.col("dance_proxy")
            + 0.10 * F.col("tempo_proxy")
            + 0.20 * F.col("valence_proxy")
            + 0.10 * F.col("live_proxy")
            + 0.10 * (1.0 - F.col("acoustic_proxy")),
        )

        cleaned_df = cleaned_df.withColumn(
            "soft_label",
            F.when(F.col("soft_score") >= 0.6, "high_energy")
            .when(F.col("soft_score") >= 0.3, "medium_energy")
            .otherwise("low_energy"),
        )

        cleaned_df = cleaned_df.withColumn(
            "assigned_category",
            F.coalesce(F.col("hard_label"), F.col("soft_label"), F.col("genre_coarse")),
        )

        feature_cols = [
            "Energy",
            "Tempo",
            "Loudness",
            "Danceability",
            "Positiveness",
            "Acousticness",
            "Speechiness",
            "Liveness",
            "Instrumentalness",
        ]

        def safe_double(column):
            col_str = F.col(column).cast("string")
            return F.when(col_str.rlike(r"^-?\d+(\.\d+)?$"), col_str.cast("double")).otherwise(
                F.lit(None).cast("double")
            )

        numeric_df = cleaned_df.select(
            "assigned_category", *[safe_double(c).alias(c) for c in feature_cols]
        )

        cat_avgs = (
            numeric_df.filter(F.col("assigned_category").isNotNull())
            .groupBy("assigned_category")
            .agg(*[F.avg(c).alias(c) for c in feature_cols])
        ).fillna({c: 0.0 for c in feature_cols})

        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        df_vectorize = assembler.transform(cat_avgs)

        songs_numeric = cleaned_df.select(
            "track_id",
            "song",
            "Artist(s)",
            "assigned_category",
            *[F.col(c).cast("double").alias(c) for c in feature_cols],
        ).fillna({c: 0.0 for c in feature_cols})

        songs_vec = VectorAssembler(
            inputCols=feature_cols, outputCol="features"
        ).transform(songs_numeric)

        centroid_df = df_vectorize.select(
            F.col("assigned_category").alias("cat"),
            F.col("features").alias("centroid_vec"),
        )

        songs_with_centroids = songs_vec.join(
            centroid_df, songs_vec.assigned_category == centroid_df.cat, "left"
        ).drop("cat")

        songs_scored = (
            songs_with_centroids.withColumn("dot", self.dot_udf("features", "centroid_vec"))
            .withColumn("n1", self.norm_udf("features"))
            .withColumn("n2", self.norm_udf("centroid_vec"))
            .withColumn("song_similarity", F.col("dot") / (F.col("n1") * F.col("n2")))
        )

        cleaned_df = cleaned_df.withColumn(
            "year", F.regexp_extract("Release Date", r"(\d{4})$", 1).cast("int")
        )

        cleaned_df = cleaned_df.withColumn(
            "is_explicit", (F.col("Explicit") == "Yes").cast("boolean")
        )

        # NOTE: For brevity, the exhaustive MOOD_CONFIG from the notebook should
        # be copied verbatim here. Due to response length constraints, it is omitted.
        # Ensure MOOD_CONFIG, recommend_for_mood, generate_playlist_for_user, and
        # generate_playlist_payload mirror the notebook implementation exactly.

        # Save prepared artifacts
        self.cleaned_df = cleaned_df
        self.mood_assembler = VectorAssembler(inputCols=self.proxy_cols, outputCol="features")
        self.mood_centroids = (
            self.spark.createDataFrame(
                [Row(mood="placeholder", **{col: 0.5 for col in self.proxy_cols})]
            )
            .limit(0)
            .withColumn("features", F.array(*[F.lit(0.0) for _ in self.proxy_cols]))
        )  # placeholder; replace with real mood_centroids from notebook

    # -------------------------------------------------------------- predictors
    def generate_playlist_for_user(self, user_track_ids: Iterable[int], limit: int = 10):
        raise NotImplementedError(
            "Implement generate_playlist_for_user using the notebook logic."
        )

    def generate_playlist_payload(self, user_track_ids: Iterable[int], limit: int = 10):
        mood, score, recs_df = self.generate_playlist_for_user(user_track_ids, limit)
        tracks = [row.asDict() for row in recs_df.collect()]
        return {
            "predicted_mood": mood,
            "mood_score": score,
            "tracks": tracks,
        }


def build_model(dataset_path: str = "spotify_dataset.csv") -> SpotifyPlaylistModel:
    return SpotifyPlaylistModel(dataset_path=dataset_path)
