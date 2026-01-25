import os
import sys
from datetime import date, timedelta
import gc
import re
import unicodedata

try:
    from data_pipeline._02_utils.utils import *
except ImportError:
    current_dir = os.path.dirname(__file__)
    config_path = os.path.abspath(os.path.join(current_dir, '..', '..', '..'))
    if config_path not in sys.path:
        sys.path.insert(0, config_path)
    from data_pipeline._02_utils.utils import *

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import NearestNeighbors
import pyarrow.fs as fs
import pyarrow.dataset as ds
import numpy as np


# ======================================================
# MAIN FUNCTION
# ======================================================
def _030202_customer_search_keynormalize(etl_date=None):
    try:
        # ==================================================
        # 0. ETL DATE
        # ==================================================
        if etl_date is None:
            if os.getenv("AIRFLOW_HOME"):
                raise ValueError("etl_date must be provided when running via Airflow")
            etl_date = (date.today() - timedelta(days=1)).strftime("%Y%m%d")

        etl_date = str(etl_date)
        print(f"Processing etl_date={etl_date}")

        # ==================================================
        # 1. PATHS
        # ==================================================
        SRC_PATH = f'{S3_DATALAKE_PATH.replace("s3a://", "")}/bronze/customer_search'
        MOVIE_PATH = f"{S3_DATALAKE_PATH}/crawl_data/rophim_all_movie_movies"
        SILVER_PATH = f"{S3_DATALAKE_PATH}/silver/customer_search_keynormalize"

        # ==================================================
        # 2. LOAD MOVIES (SMALL)
        # ==================================================
        print("Loading movie data...")
        movies = pd.read_parquet(
            MOVIE_PATH,
            engine="pyarrow",
            columns=["slug", "title", "genres"]
        )

        movies.columns = movies.columns.str.lower().str.strip()
        movies["slug"] = movies["slug"].astype(str)
        movies["title"] = movies["title"].astype(str)

        def normalize_genres(v):
            parts = re.split(r"[|,/;]", str(v))
            return "; ".join(p.strip() for p in parts if p.strip())

        movies["genres_normalized"] = movies["genres"].apply(normalize_genres)
        movies = movies.drop(columns=["genres"])

        print(f"✅ Movies loaded: {len(movies):,}")

        # ==================================================
        # 3. LOAD CUSTOMER SEARCH (STREAM)
        # ==================================================
        print("Loading customer search data...")

        s3_fs = fs.S3FileSystem(region=os.getenv("AWS_REGION", "ap-southeast-1"))
        dataset = ds.dataset(SRC_PATH, format="parquet", filesystem=s3_fs)

        batch_size = 50_000
        chunks = []

        for batch in dataset.to_batches(batch_size=batch_size):
            df = batch.to_pandas()
            df.columns = df.columns.str.lower().str.strip()

            df["date_key"] = (
                df["datetime"]
                .astype(str)
                .str.slice(0, 10)
                .str.replace("-", "")
            )

            df = df[df["date_key"] == etl_date]

            if not df.empty:
                chunks.append(df)

            del df
            gc.collect()

        if not chunks:
            print("No data found → write empty result")
            pd.DataFrame().to_parquet(SILVER_PATH, engine="pyarrow", index=False)
            return True

        keywords = pd.concat(chunks, ignore_index=True)
        del chunks
        gc.collect()

        print(f"✅ Keywords loaded: {len(keywords):,}")

        # ==================================================
        # 4. SLUG NORMALIZATION
        # ==================================================
        def remove_accents(s):
            return (
                unicodedata.normalize("NFKD", str(s))
                .encode("ascii", "ignore")
                .decode("utf-8")
            )

        keywords["keyword_slug"] = (
            keywords["keyword"]
            .apply(remove_accents)
            .str.lower()
            .str.strip()
            .str.replace(r"[^a-z0-9]+", "-", regex=True)
            .str.replace(r"(^-|-$)", "", regex=True)
        )

        keywords = keywords[keywords["keyword_slug"] != ""]
        print(f"✅ Valid slugs: {len(keywords):,}")

        # ==================================================
        # 5. TF-IDF (FLOAT32)
        # ==================================================
        print("Building TF-IDF vectors...")

        vectorizer = TfidfVectorizer(
            analyzer="char",
            ngram_range=(2, 3),
            min_df=2,
            max_df=0.95,
            max_features=30_000
        )

        movie_vectors = vectorizer.fit_transform(movies["slug"]).astype("float32")
        keyword_vectors = vectorizer.transform(keywords["keyword_slug"]).astype("float32")

        print("✅ TF-IDF done")

        # ==================================================
        # 6. NEAREST NEIGHBORS (STREAM SAFE)
        # ==================================================
        print("Finding nearest neighbors...")

        nn = NearestNeighbors(
            n_neighbors=1,
            metric="cosine",
            algorithm="brute",
            n_jobs=1
        )
        nn.fit(movie_vectors)

        batch_knn = 10_000

        keyword_category = []
        keyword_normalized = []
        keyword_normalized_slug = []

        for i in range(0, keyword_vectors.shape[0], batch_knn):
            batch_vec = keyword_vectors[i:i + batch_knn]
            distances, indices = nn.kneighbors(batch_vec)

            for d, idx in zip(distances.flatten(), indices.flatten()):
                score = 1 - float(d)
                if score >= 0.5:
                    keyword_category.append(movies.iloc[idx]["genres_normalized"])
                    keyword_normalized.append(movies.iloc[idx]["title"])
                    keyword_normalized_slug.append(movies.iloc[idx]["slug"])
                else:
                    keyword_category.append("not_matched")
                    keyword_normalized.append("not_matched")
                    keyword_normalized_slug.append("not_matched")

            del batch_vec, distances, indices
            gc.collect()

        print("✅ KNN mapping done")

        # ==================================================
        # 7. FINALIZE
        # ==================================================
        keywords["keyword_category"] = keyword_category
        keywords["keyword_normalized"] = keyword_normalized
        keywords["keyword_normalized_slug"] = keyword_normalized_slug

        keywords = keywords.rename(columns={
            "eventid": "event_id",
            "datetime": "date_log",
            "keyword": "original_keyword",
            "keyword_slug": "original_keyword_slug"
        })

        keywords.to_parquet(
            SILVER_PATH,
            engine="pyarrow",
            index=False,
            compression="snappy"
        )

        print(f"✅ SUCCESS: {len(keywords):,} rows written")
        return True

    except Exception as e:
        print("❌ JOB FAILED:", e)
        raise

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--etl_date", required=True)
    args = parser.parse_args()
    _030202_customer_search_keynormalize(args.etl_date)
