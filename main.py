import asyncio
import boto3
from dotenv import load_dotenv
from fastapi import FastAPI
import json
import os
import redis.asyncio as redis
import time
import tmdbsimple as tmdb # type: ignore
from typing import Any, Optional

load_dotenv(dotenv_path=".env.development")
app = FastAPI()

NUM_MOVIES = 10000 # TODO change to 10000
REDIS_TTL = 60 * 60 * 24 * 7 # one week

tmdb.API_KEY = os.getenv("TMDB_API_KEY")

redis_client = redis.from_url(os.getenv("REDIS_URL")) # type: ignore

async def get_popular_movie_ids() -> list[int]:
    movie_instance = tmdb.Movies()
    num_pages = (NUM_MOVIES + 19) // 20
    popular_movie_ids: list[int] = []

    for i in range(num_pages):
        try:
            # First check if each popular page has been cached
            cache_key_popular_page = f"tmdb:popular:page:{i + 1}"
            cache_result_popular_page: Optional[str] = await redis_client.get(cache_key_popular_page)
            popular_page: list[dict[str, Any]]
            if cache_result_popular_page is not None: # cache hit
                popular_page = json.loads(cache_result_popular_page)
            else: # cache miss
                response = await asyncio.to_thread(movie_instance.popular, page=(i + 1)) # type: ignore
                popular_page = response["results"]
                # Save to Redis cache
                await redis_client.setex(cache_key_popular_page, REDIS_TTL, json.dumps(popular_page))

            popular_movie_ids.extend([movie["id"] for movie in popular_page])
        except Exception as e:
            print(e)
            break
    return popular_movie_ids

async def perform_data_ingestion():
    popular_movie_ids = await get_popular_movie_ids()
    popular_movie_ids = list(set(popular_movie_ids)) # Remove duplicate id's

    print(f"A total of {len(popular_movie_ids)} movie id's have been fetched.")

    num_movies_hydrated = 0

    """
    For each movie id, fetch movie data
    Important movie metadata includes movie_id, title, release_year, genres,
    average_rating, popularity_score, cast, director
    """
    for movie_id in popular_movie_ids:
        try:
            # First check if each movie instance has been cached (as a dict)
            cache_key_movie_instance = f"tmdb:movie:{movie_id}"
            cache_result_movie_instance = await redis_client.get(cache_key_movie_instance)
            movie_dict: dict[str, Any]
            if cache_result_movie_instance is not None: # cache hit
                movie_dict = json.loads(cache_result_movie_instance)
            else: # cache miss
                # This movie's details have not been fetched recently
                movie: tmdb.Movies = await asyncio.to_thread(tmdb.Movies, movie_id)
                await asyncio.to_thread(movie.info) # type: ignore
                await asyncio.to_thread(movie.credits) # type: ignore
                movie_dict = vars(movie)
                # Save to Redis cache
                await redis_client.setex(cache_key_movie_instance, REDIS_TTL, json.dumps(movie_dict))
            
            num_movies_hydrated += 1

            # if movie_id == popular_movie_ids[0]:
            #     print(movie_dict)

            # TODO save to S3

        except Exception as e:
            print(e)
            continue
    
    print(f"Movie details for {num_movies_hydrated} movies have been fetched.")
    



@app.get("/admin/data_pipeline") # TODO change to POST, add verification
async def execute_data_pipeline():
    start_time = time.time()
    await perform_data_ingestion()
    end_time = time.time()
    print(f"Data ingestion ran in {end_time - start_time} seconds")

    # TODO chunking and embedding into ChromaDB

    return {"status": "attempted"}

@app.get("/")
async def root():
    return {"message": "API for Movie Guru Backend"}

@app.get("/status")
async def get_status():
    return {"status": "ok"}