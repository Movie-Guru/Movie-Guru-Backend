import asyncio
import boto3
from collections.abc import Awaitable
from dotenv import load_dotenv
from fastapi import FastAPI
import json
import os
import redis
from requests.exceptions import HTTPError
import time
import tmdbsimple as tmdb # type: ignore
import traceback
from typing import Any

load_dotenv(dotenv_path=".env.development")
app = FastAPI()

NUM_MOVIES = 100
REDIS_TTL = 60 * 60 * 24 * 7 # one week
S3_MOVIES_PATH_PREFIX = "movies/"

tmdb.API_KEY = os.getenv("TMDB_API_KEY")

redis_client = redis.asyncio.from_url(os.getenv("REDIS_URL")) # type: ignore
sync_redis_client = redis.from_url(os.getenv("REDIS_URL")) # type: ignore

# AWS usage setup
aws_session = boto3.session.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)
s3 = aws_session.resource("s3") # type: ignore
bucket = s3.Bucket(os.getenv("S3_BUCKET_NAME")) # type: ignore

async def get_popular_movie_ids() -> list[int]:
    """
        Returns a list of id's for the top N most popular movies
        Uses TMDb API calls or Redis cache if available
    """
    movie_instance = tmdb.Movies()
    num_pages = (NUM_MOVIES + 19) // 20
    popular_movie_ids: list[int] = []

    for i in range(num_pages):
        try:
            # First check if each popular page has been cached
            cache_key_popular_page = f"tmdb:popular:page:{i + 1}"
            cache_result_popular_page: bytes | None = await redis_client.get(cache_key_popular_page)
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

async def delete_bucket_objects() -> None:
    """
        Deletes all the objects in the S3 bucket used in this application
        Precondition: bucket must exist
    """
    try: 
        print("Deleting objects in the S3 bucket...")
        start_time = time.time()
        # First get a list of existing object keys
        object_summaries = await asyncio.to_thread(bucket.objects.all) # type: ignore
        objects: list[dict[str, str]] = [
            {"Key": object_summary.key} # type: ignore
            for object_summary in object_summaries # type: ignore
        ]

        # Each "delete" API call can process at most MAX_KEYS keys
        # This is a value set by AWS
        MAX_KEYS = 1000
        for i in range((len(objects) + MAX_KEYS - 1) // MAX_KEYS):
            # Make API call to delete objects
            await asyncio.to_thread(
                bucket.delete_objects, # type: ignore
                Delete={
                    "Objects": objects[MAX_KEYS * i: MAX_KEYS * (i + 1)]
                },
                ExpectedBucketOwner=os.getenv("AWS_ACCOUNT_ID")
            )
        end_time = time.time()
        print(f"Successfully deleted {len(objects)} objects in the S3 bucket.")
        print(f"Deletion of objects ran in {end_time - start_time} seconds")
    except Exception as e:
        print(e)

async def prepare_bucket() -> None:
    """
    Creates a bucket if it does not exist, or empties the bucket if it exists
    """
    print("Attempting to create S3 bucket...")
    try:
        await asyncio.to_thread(bucket.create, CreateBucketConfiguration={ # type: ignore
            "LocationConstraint": os.getenv("AWS_REGION")
        })
        print("Created a new S3 bucket.")
    except Exception as e:
        if hasattr(e, "response") and "Error" in e.response and "Code" in e.response["Error"]: # type: ignore
            error_code: str = e.response["Error"]["Code"] # type: ignore
            if error_code == "BucketAlreadyOwnedByYou":
                print("The S3 Bucket has been created previously.")
                await delete_bucket_objects()
            else:
                traceback.print_exc()
        else:
            traceback.print_exc()

def hydrate_and_store_movie_details(movie_id: int) -> None:
    """
    Synchronous function that represents one logical unit of work (task)
    Hydrates movie details for the specified movie ID via TMDb API or Redis
    Then uploads raw movie data to S3
    May thrown an exception
    """
    # First check if each movie instance has been cached (as a JSON string)
    cache_key_movie_instance = f"tmdb:movie:{movie_id}"
    cache_result_movie_instance: bytes | None = sync_redis_client.get(cache_key_movie_instance) # type: ignore
    movie_data_bytes: bytes
    if cache_result_movie_instance is not None: # cache hit
        movie_data_bytes = cache_result_movie_instance # type: ignore
    else: # cache miss
        # This movie's details have not been fetched recently
        # Important movie metadata to fetch includes movie_id, title, release_year,
        # genres, average_rating, popularity_score, cast, director
        movie = tmdb.Movies(movie_id)
        # Populate attributes of the movie instance
        movie.info() # type: ignore
        movie.credits() # type: ignore
        movie_data_bytes = json.dumps(vars(movie)).encode("utf-8")
        # Save to Redis cache
        sync_redis_client.setex(cache_key_movie_instance, REDIS_TTL, movie_data_bytes)

    # Store the movie data in S3 bucket
    bucket_key = f"{S3_MOVIES_PATH_PREFIX}{movie_id}"
    bucket.put_object(Key=bucket_key, Body=movie_data_bytes) # type: ignore


async def perform_data_ingestion() -> None:
    """
        Fetches a list of movie id's, hydrates movie details via further TMDb 
        API calls or Redis Caching, and uploads movie data to S3
    """
    print("Fetching movie id's...")
    start_time = time.time()
    popular_movie_ids = await get_popular_movie_ids()
    popular_movie_ids = list(set(popular_movie_ids)) # Remove duplicate id's
    end_time = time.time()
    print(f"Fetching movie id's ran in {end_time - start_time} seconds")
    print(f"A total of {len(popular_movie_ids)} movie id's have been fetched.")

    # Set up S3 bucket for data storage
    await prepare_bucket()

    client = boto3.client('s3') # type: ignore
    bucket_location: dict[str, Any] = await asyncio.to_thread(
        client.get_bucket_location, # type: ignore
        Bucket=os.getenv("S3_BUCKET_NAME"),
        ExpectedBucketOwner=os.getenv("AWS_ACCOUNT_ID")
    )
    print(bucket_location)

    await asyncio.to_thread(bucket.load) # type: ignore
    print(bucket.bucket_arn) # type: ignore
    print(bucket.bucket_region) # type: ignore
    print(bucket.creation_date) # type: ignore
    
    # For each movie id, fetch movie details and store data in the S3 bucket
    print("Fetching movie details and storing movie data in an S3 bucket...")
    start_time = time.time()
    movie_tasks: list[Awaitable[None]] = []
    for movie_id in popular_movie_ids:
        movie_task: Awaitable[None] = asyncio.to_thread(hydrate_and_store_movie_details, movie_id)
        movie_tasks.append(movie_task)

    # Gather the results of the hydration and storage tasks
    movie_tasks_results: list[BaseException | None] = await asyncio.gather(*movie_tasks, return_exceptions=True)
    end_time = time.time()
    print(f"Movie data hydration and storage ran in {end_time - start_time} seconds")
    
    # Process results of the batched tasks
    num_movies_processed = 0
    for res in movie_tasks_results:
        if isinstance(res, Exception):
            e = res
            print("Task failed:")
            if isinstance(e, HTTPError):
                print("Unsuccessful TMDb API call to fetch movie data")
                print(e)
            else:
                print(e)
                traceback.print_exc()
        else:
            num_movies_processed += 1
    print(
        f"Out of {len(movie_tasks_results)} movies, movie details for "
        f"{num_movies_processed} movies have been fetched and stored in S3."
    )

@app.get("/admin/data_pipeline") # TODO change to POST, add verification
async def execute_data_pipeline():
    print("Initiating data pipeline.")
    start_time = time.time()
    await perform_data_ingestion()
    end_time = time.time()
    print(f"Full data ingestion pipeline ran in {end_time - start_time} seconds")

    # TODO chunking and embedding into ChromaDB

    return {"status": "attempted"}

@app.get("/")
async def root():
    return {"message": "API for Movie Guru Backend"}

@app.get("/status")
async def get_status():
    return {"status": "ok"}