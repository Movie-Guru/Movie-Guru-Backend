import asyncio
import boto3
from collections.abc import Awaitable
from datetime import datetime
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, status
import json
from openai import OpenAI
import os
from pprint import pprint # type: ignore
from pydantic import BaseModel
import redis
from requests.exceptions import HTTPError
import time
import tmdbsimple as tmdb # type: ignore
import traceback
from typing import Any

load_dotenv(dotenv_path=".env.development")
app = FastAPI()

NUM_MOVIES = 10000
REDIS_TTL = 60 * 60 * 24 * 7 # one week
S3_MOVIES_PATH_PREFIX = "movies/"

tmdb.API_KEY = os.getenv("TMDB_API_KEY")

# Redis setup
redis_client = redis.asyncio.from_url(os.getenv("REDIS_URL")) # type: ignore
sync_redis_client = redis.from_url(os.getenv("REDIS_URL")) # type: ignore

# AWS usage setup
aws_session = boto3.session.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)
s3_client = aws_session.client("s3") # type: ignore

# OpenAI setup
openai_client = OpenAI()

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

def delete_bucket_objects_sync() -> None:
    """
        Deletes all the objects in the S3 bucket used in this application
        Precondition: bucket must exist
    """
    try: 
        print("Deleting objects in the S3 bucket...")
        start_time = time.time()

        # Use a paginator to loop through the pages of objects in the bucket
        paginator = s3_client.get_paginator("list_objects_v2") # type: ignore
        page_iterator = paginator.paginate(Bucket=os.getenv("S3_BUCKET_NAME")) # type: ignore

        num_objects = 0
        for page in page_iterator: # type: ignore
            # Generate a list of object dicts to use in the delete request
            objects: list[dict[str, str]] = [{"Key": obj["Key"]} for obj in page["Contents"]] # type: ignore
            num_objects += len(objects)

            # Make a delete API request using this list of objects
            s3_client.delete_objects( # type: ignore
                Bucket=os.getenv("S3_BUCKET_NAME"),
                Delete={
                    "Objects": objects
                },
                ExpectedBucketOwner=os.getenv("AWS_ACCOUNT_ID")
            )

        end_time = time.time()
        print(f"Successfully deleted {num_objects} objects in the S3 bucket.")
        print(f"Deletion of objects ran in {end_time - start_time} seconds")
    except Exception:
        traceback.print_exc()

async def prepare_bucket() -> None:
    """
    Creates a bucket if it does not exist, or empties the bucket if it exists
    """
    print("Attempting to create S3 bucket...")
    try:
        await asyncio.to_thread(
            s3_client.create_bucket, # type: ignore
            Bucket=os.getenv("S3_BUCKET_NAME"),
            CreateBucketConfiguration={
                "LocationConstraint": os.getenv("AWS_REGION")
            }
        )
        print("Created a new S3 bucket.")
    except Exception as e:
        if hasattr(e, "response") and "Error" in e.response and "Code" in e.response["Error"]: # type: ignore
            error_code: str = e.response["Error"]["Code"] # type: ignore
            if error_code == "BucketAlreadyOwnedByYou":
                print("The S3 Bucket has been created previously.")
                await asyncio.to_thread(delete_bucket_objects_sync)
            else:
                traceback.print_exc()
        else:
            traceback.print_exc()

def hydrate_and_store_movie_details_sync(movie_id: int) -> None:
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
        # genres, average_rating, popularity_score, cast, directors, revenue, runtime
        movie = tmdb.Movies(movie_id)
        # Populate attributes of the movie instance
        movie.info() # type: ignore
        movie.credits() # type: ignore
        movie_data_bytes = json.dumps(vars(movie)).encode("utf-8")
        # Save to Redis cache
        sync_redis_client.setex(cache_key_movie_instance, REDIS_TTL, movie_data_bytes)

    # Store the movie data in S3 bucket
    bucket_key = f"{S3_MOVIES_PATH_PREFIX}{movie_id}"
    s3_client.put_object( # type: ignore
        Bucket=os.getenv("S3_BUCKET_NAME"),
        Key=bucket_key,
        Body=movie_data_bytes
    )

async def perform_data_acquisition() -> None:
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

    # For each movie id, fetch movie details and store data in the S3 bucket
    print("Fetching movie details and storing movie data in an S3 bucket...")
    start_time = time.time()
    movie_tasks: list[Awaitable[None]] = []
    for movie_id in popular_movie_ids:
        movie_task: Awaitable[None] = asyncio.to_thread(
            hydrate_and_store_movie_details_sync,
            movie_id
        )
        movie_tasks.append(movie_task)

    # Gather the results of the hydration and storage tasks
    movie_tasks_results: list[BaseException | None] = await asyncio.gather(
        *movie_tasks,
        return_exceptions=True
    )
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

x: int = 0

def index_single_movie_sync(object_key: str) -> None:
    """
        Takes in an object key for one of the movie objects in the S3 bucket
        Uses the key to extract the corresponding movie data from the bucket
        Performs chunking and generate embeddings for the movie
        Stores embeddings in ChromaDB
    """
    global x
    x += 1

    # Fetch the object from S3
    obj_response: dict[str, Any] = s3_client.get_object( # type: ignore
        Bucket=os.getenv("S3_BUCKET_NAME"),
        Key=object_key
    )
    
    # Decode the S3 object response into a local dict
    movie = json.loads(obj_response["Body"].read().decode("utf-8")) # type: ignore

    try:
        # Parse important metadata from the movie dict
        movie_id: int = movie["id"]
        title: str = movie["title"]
        release_year: int = datetime.fromisoformat(movie["release_date"]).year
        genres: str = ", ".join([obj["name"] for obj in movie["genres"]])
        average_rating: float = movie["vote_average"]
        popularity_score: float = movie["popularity"]
        cast: str = ", ".join([member["name"] for member in movie["cast"]])
        directors: str = ", ".join(
            [member["name"]
            for member in movie["crew"]
            if member["job"].lower() == "director"]
        )
        revenue: int = movie["revenue"]
        runtime: int = movie["runtime"]
        overview: str = movie["overview"]

        print(movie_id)
        print(title)
        print(release_year)
        print(genres)
        print(average_rating)
        print(popularity_score)
        print(cast)
        print(directors)
        print(revenue)
        print(runtime)
        print(overview)

        # Create embeddings for each of the text attributes
        input = [
            f"Cast: {cast}",
            f"Director: {directors}",
            f"Overview: {overview}"
        ]

        response = openai_client.embeddings.create(
            model="text-embedding-3-small",
            input=input
        )
        for embedding_obj in response.data:
            print(len(embedding_obj.embedding))

        print("CUTOFF")
    except Exception:
        traceback.print_exc()

def get_indexing_tasks_sync() -> list[Awaitable[None]]:
    """
        Uses the S3 bucket to return a list of indexing tasks, one for each movie
    """
    paginator = s3_client.get_paginator("list_objects_v2") # type: ignore
    page_iterator = paginator.paginate(Bucket=os.getenv("S3_BUCKET_NAME")) # type: ignore

    indexing_tasks: list[Awaitable[None]] = []

    for page in page_iterator: # type: ignore
        for obj in page["Contents"]: # type: ignore
            indexing_task: Awaitable[None] = asyncio.to_thread(
                index_single_movie_sync,
                obj["Key"] # type: ignore
            )
            indexing_tasks.append(indexing_task)
    
    return indexing_tasks

async def perform_data_indexing() -> None:
    """
        Gets the movie data from the S3 bucket into the ChromaDB database
    """
    print("Running data indexing pipeline...")
    start_time = time.time()

    indexing_tasks = await asyncio.to_thread(get_indexing_tasks_sync)

    indexing_tasks_results: list[BaseException | None] = await asyncio.gather(
        indexing_tasks[0],
        return_exceptions=True
    )

    print(f"There are {len(indexing_tasks_results)} task results.")

    end_time = time.time()
    print(f"Data indexing pipeline ran in {end_time - start_time} seconds")

class ExecuteDataPipelineRequest(BaseModel):
    skip_data_acquisition: bool = False
    password: str # Potential improvement: use auth headers

@app.post("/admin/data_pipeline")
async def execute_data_pipeline(payload: ExecuteDataPipelineRequest) -> dict[str, str]:
    try:
        if payload.password != os.getenv("ADMIN_API_KEY"):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect password"
            )
        
        print("Initiating data pipeline.")

        if payload.skip_data_acquisition:
            print("You have requested to skip the data acquisition pipeline.")
        else:        
            print("Initiating data acquisition pipeline.")
            start_time = time.time()
            await perform_data_acquisition()
            end_time = time.time()
            print(f"Full data acquisition pipeline ran in {end_time - start_time} seconds")

        # Chunking and embedding into ChromaDB
        await perform_data_indexing()
        
        print("Data pipeline is complete.")
        return {"status": "complete"}
    except Exception:
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error has occurred."
        )

@app.get("/")
async def root() -> dict[str, str]:
    return {"message": "API for Movie Guru Backend"}

@app.get("/status")
async def get_status() -> dict[str, str]:
    return {"status": "ok"}