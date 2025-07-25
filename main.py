from dotenv import load_dotenv
from fastapi import FastAPI
import json
import os
import redis.asyncio as redis
import tmdbsimple as tmdb # type: ignore
import traceback
from typing import Any, Optional

load_dotenv(dotenv_path=".env.development")
app = FastAPI()

NUM_MOVIES = 120 # TODO change to 10000
REDIS_TTL = 60 * 60 * 24 * 7 # one week

tmdb.API_KEY = os.getenv("TMDB_API_KEY")

redis_client = redis.from_url(os.getenv("REDIS_URL")) # type: ignore

async def get_popular_movie_ids() -> list[int]:
    movie_instance = tmdb.Movies()
    num_pages = (NUM_MOVIES + 19) // 20
    popular_movie_ids: list[int] = []

    for i in range(num_pages):
        try:
            cache_key_popular_page = f"popular:page:{i + 1}"
            cache_result_popular_page: Optional[str] = await redis_client.get(cache_key_popular_page)
            popular_page: list[dict[str, Any]]
            if cache_result_popular_page is not None: # cache hit
                print("Cache hit")
                popular_page = json.loads(cache_result_popular_page)
            else: # cache miss
                # TODO change to async API call
                print("Cache miss")
                response = movie_instance.popular(page=(i + 1)) # type: ignore
                popular_page = response["results"]
                await redis_client.setex(cache_key_popular_page, REDIS_TTL, json.dumps(popular_page))

            popular_movie_ids.extend([movie["id"] for movie in popular_page])
        except Exception:
            traceback.print_exc()
            break
    return popular_movie_ids

async def perform_data_ingestion():
    popular_movie_ids = await get_popular_movie_ids()

    print(f"A total of {len(popular_movie_ids)} movies have been loaded.")
    print(popular_movie_ids[0:5])

    # For each movie id, fetch movie data and save to S3

    # TODO hydrate data
    # TODO save to S3

@app.get("/data_pipeline") # TODO Find better method
async def execute_data_pipeline():
    await perform_data_ingestion()
    # TODO chunking and embedding into ChromaDB
    return {"status": "attempted"}

@app.get("/")
async def root():
    return {"message": "API for Movie Guru Backend"}

@app.get("/status")
async def get_status():
    return {"status": "ok"}