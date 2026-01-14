import os, sys
current_dir = os.path.dirname(__file__)
config_path = os.path.join(current_dir, '..','..')
config_path = os.path.abspath(config_path)
sys.path.insert(0, config_path)
from _01_config.jar_paths import *
from _02_utils.utils import *
from datetime import date
import pandas as pd
import re
import unicodedata
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import NearestNeighbors


def _030202_customer_search_keynormalize(etl_date=None):
    try:
        # Default etl_date = today
        if etl_date is None:
            etl_date = date.today().strftime("%Y%m%d")
        else:
            etl_date = str(etl_date)
        # ======================================================
        # 1. Define S3 paths
        # ======================================================

        SRC_PATH = f"{S3_DATALAKE_PATH}/bronze/customer_search"

        MOVIE_PATH = f"{S3_DATALAKE_PATH}/crawl_data/rophim_all_movie_movies"

        SILVER_PATH = f"{S3_DATALAKE_PATH}/silver/customer_search_keynormalize"


        # ======================================================
        # 2. Load data from S3
        # ======================================================

        # Load customer search data (bronze layer)
        keywords = pd.read_parquet(
            SRC_PATH,
            engine="pyarrow"
        )

        # Load crawled movie data
        movies = pd.read_parquet(
            MOVIE_PATH,
            engine="pyarrow"
        )

        # Normalize column names
        keywords.columns = keywords.columns.str.strip().str.lower()
        movies.columns   = movies.columns.str.strip().str.lower()

        # Filter by etl_date
        keywords["date_key"] = (
            keywords["datetime"]
            .astype(str)
            .str.slice(0, 10)
            .str.replace("-", "")
        )
        keywords = keywords[keywords["date_key"] == etl_date]

        # create slug column
        def remove_accents(s):
            if pd.isna(s):
                return None
            return (
                unicodedata
                .normalize("NFKD", s)
                .encode("ascii", "ignore")
                .decode("utf-8")
            )
        
        keywords["keyword_slug"] = (
            keywords["keyword"]
                .apply(remove_accents)              # Remove accents
                .str.lower()                         # lower
                .str.strip()                         # trim
                .str.replace(r"[^a-z0-9]+", "-", regex=True)  # special chars → -
                .str.replace(r"(^-|-$)", "", regex=True)     # trim -
        )

        # Remove null keyword_slug
        keywords["keyword_slug"] = keywords["keyword_slug"].astype(str)

        # Validate required columns
        required_movie_cols = ["slug", "title", "genres"]
        for col in required_movie_cols:
            if col not in movies.columns:
                raise Exception(f"Movie data is missing required column: {col}")

        if "keyword_slug" not in keywords.columns:
            raise Exception("Keyword data is missing column: keyword_slug")

        # Cast to string
        keywords["keyword_slug"] = keywords["keyword_slug"].astype(str)
        movies["slug"]  = movies["slug"].astype(str)
        movies["title"] = movies["title"].astype(str)


        # ======================================================
        # 3. Normalize genres column
        # ======================================================

        def normalize_genres(value: str) -> str:
            """
            Split genres by common separators and re-join using '; '
            """
            value = str(value)
            parts = re.split(r"[|,/;]", value)
            parts = [g.strip() for g in parts if g.strip()]
            return "; ".join(parts)

        movies["genres_normalized"] = movies["genres"].apply(normalize_genres)


        # ======================================================
        # 4. TF-IDF vectorization on slug (character n-gram)
        # ======================================================

        vectorizer = TfidfVectorizer(
            analyzer="char",
            ngram_range=(2, 4),
            min_df=1,
            max_features=200_000
        )

        movie_vectors = vectorizer.fit_transform(movies["slug"])
        keyword_vectors = vectorizer.transform(keywords["keyword_slug"])


        # ======================================================
        # 5. Nearest Neighbor search (cosine similarity)
        # ======================================================

        nn = NearestNeighbors(
            n_neighbors=1,
            metric="cosine",
            algorithm="brute",
            n_jobs=-1
        )

        nn.fit(movie_vectors)

        distances, indices = nn.kneighbors(keyword_vectors)

        similarity_scores = (1 - distances).flatten()
        best_indices = indices.flatten()


        # ======================================================
        # 6. Map matching result back to keywords
        # ======================================================

        keyword_category     = []
        keyword_normalized   = []
        keyword_normalized_slug = []

        for score, idx in zip(similarity_scores, best_indices):

            score = float(score)

            movie_slug   = movies.iloc[idx]["slug"]
            movie_title  = movies.iloc[idx]["title"]
            movie_genres = movies.iloc[idx]["genres_normalized"]

            # Trust ML result only if similarity is high enough
            """
            if score >= 0.5 -> trust
            """
            if score >= 0.5:
                keyword_category.append(movie_genres)
                keyword_normalized.append(movie_title)
                keyword_normalized_slug.append(movie_slug)
            else:
                keyword_category.append("not_matched")
                keyword_normalized.append("not_matched")
                keyword_normalized_slug.append("not_matched")


        keywords["keyword_category"]   = keyword_category
        keywords["keyword_normalized"]  = keyword_normalized
        keywords["keyword_normalized_slug"] = keyword_normalized_slug

        cols = ["eventid","date_key","datetime","user_id","keyword","keyword_slug","keyword_normalized","keyword_normalized_slug"
                ,"keyword_category","category","proxy_isp","platform","networktype","action"]
        
        keywords = keywords[cols]\
            .rename(columns={
                "eventid": "event_id",
                "datetime": "date_log",
                "keyword": "original_keyword",
                "keyword_slug": "original_keyword_slug"
            })
        
        """
            if user_id = null -> user_id = 00000000
        """

        keywords["user_id"] = (
            keywords["user_id"]
            .fillna("00000000")
            .astype("string")
        )

        # Data Type Standardization
        keywords = (
            keywords
            .astype({
                "event_id": "string",
                "date_key": "string",
                "user_id": "string",
                "original_keyword": "string",
                "original_keyword_slug":"string",
                "keyword_normalized": "string",
                "keyword_normalized_slug": "string",
                "keyword_category": "string",
                "category": "string",
                "proxy_isp": "string",
                "platform": "string",
                "networktype": "string",
                "action": "string"
            })
            .assign(
                date_log=lambda x: (
                    pd.to_datetime(x["date_log"], errors="coerce")
                    .dt.floor("s")
                    .astype("datetime64[s]")
                )
            )
        )

        # ======================================================
        # 7. Write Silver output to S3 (Parquet)
        # ======================================================
        print("===== Loading data to Silver Layer... =====")
        keywords.to_parquet(
            SILVER_PATH,
            engine="pyarrow",
            index=False
        )

        print("===== ✅ Load data to silver.customer_search_keynormalize successfully !... =====")
        return True

    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='_030202_customer_search_keynormalize')
    parser.add_argument('--etl_date', type=int, help='etl_date (YYYYMMDD)')
    args = parser.parse_args()

    success = _030202_customer_search_keynormalize(etl_date=args.etl_date)
    exit(0 if success else 1)
