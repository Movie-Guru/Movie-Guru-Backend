import asyncio
import boto3
import chromadb
from chromadb.errors import NotFoundError
from chromadb.types import Metadata, Where
from chromadb.utils.embedding_functions import OpenAIEmbeddingFunction
from collections.abc import Awaitable
from datetime import datetime
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, status
import json
from openai import OpenAI
import os
from pprint import pprint # type: ignore
from pydantic import BaseModel, field_validator
import redis
from requests.exceptions import HTTPError
import time
import tmdbsimple as tmdb # type: ignore
import traceback
from typing import Any, cast

load_dotenv(dotenv_path=".env.development")
app = FastAPI()

# Data ingestion pipeline
NUM_MOVIES = 10000
REDIS_TTL = 60 * 60 * 24 * 7 # one week
S3_MOVIES_PATH_PREFIX = "movies/"

# OpenAI and ChromaDB
EMBEDDING_MODEL_NAME = "text-embedding-3-small"
CHROMA_COLLECTION_NAME = "movies"
MAX_QUERY_RESULTS = 10

# Semantic key names for semantic movie attributes stored as documents 
# Must match the keys of the MovieSchema BaseModel below
# Also used for generating embedding ids
# and creating JSON that is passed to the LLM
CAST_KEY = "cast"
DIRECTORS_KEY = "directors"
OVERVIEW_KEY = "overview"
SEMANTIC_KEYS = [CAST_KEY, DIRECTORS_KEY, OVERVIEW_KEY]
KEYS_NOT_IN_METADATA = [OVERVIEW_KEY]

# DATA STRUCTURES

# Consistent schema for JSON input to LLM
# Also the metadata schema for embeddings is based on this
# When declaring or instantiating a TypedDict or BaseModel,
# the key must be a hardcoded literal
class MovieSchema(BaseModel):
    movie_id: int | None = None
    title: str | None = None
    release_year: int | None = None
    genres: list[str] | None = None
    average_rating: float | None = None
    popularity_score: float | None = None
    cast: list[str] | None = None
    directors: list[str] | None = None
    revenue: int | None = None
    runtime: int | None = None
    overview: str | None = None

    @field_validator("genres", "cast", "directors", mode="before")
    @classmethod
    def split_comma_separated_strings(cls, v: Any):
        # If the value is a string containing comma-separated strings
        if isinstance(v, str):
            return [item.strip() for item in v.split(",")] or None
        # Otherwise, do nothing
        print("not appearing :(")
        return v

class ExecuteDataPipelineRequest(BaseModel):
    skip_data_acquisition: bool = False
    password: str # Potential improvement: use auth headers

class DataPipelineResponse(BaseModel):
    has_completed: bool

class RecommendationPayload(BaseModel):
    user_query: str

class Recommendation(BaseModel):
    recommendation: str

# SETUP FUNCTIONS

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
openai_ef = OpenAIEmbeddingFunction(
    api_key=os.getenv("OPENAI_API_KEY"),
    model_name=EMBEDDING_MODEL_NAME
)

# ChromaDB setup
chroma_client = chromadb.PersistentClient()
# TODO: Use HTTP client for prod
# To make Chroma ready for serving API requests
chroma_collection = chroma_client.get_or_create_collection(
    name=CHROMA_COLLECTION_NAME,
    embedding_function=openai_ef # type: ignore
)

# HELPER FUNCTIONS

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
        if isinstance(res, BaseException):
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


def index_single_movie_sync(object_key: str) -> None:
    """
        Takes in an object key for one of the movie objects in the S3 bucket
        Uses the key to extract the corresponding movie data from the bucket
        Performs chunking and generate embeddings for the movie
        Stores embeddings in ChromaDB
    """
    # Fetch the object from S3
    obj_response: dict[str, Any] = s3_client.get_object( # type: ignore
        Bucket=os.getenv("S3_BUCKET_NAME"),
        Key=object_key
    )
    
    # Decode the S3 object response into a local dict
    movie_from_bucket = json.loads(obj_response["Body"].read().decode("utf-8")) # type: ignore

    # Parse important metadata from the movie dict
    release_year: int | None
    try:
        release_year = datetime.fromisoformat(movie_from_bucket["release_date"]).year
    except ValueError:
        release_year = None

    movie = MovieSchema(
        movie_id=movie_from_bucket["id"],
        title=movie_from_bucket["title"],
        release_year=release_year,
        genres=[
            obj["name"]
            for obj in movie_from_bucket["genres"]
        ] or None,
        average_rating=movie_from_bucket["vote_average"],
        popularity_score=movie_from_bucket["popularity"],
        cast=[
            member["name"]
            for member in movie_from_bucket["cast"]
        ],
        directors=[
            member["name"]
            for member in movie_from_bucket["crew"]
            if member["job"].lower() == "director"
        ] or None,
        revenue=movie_from_bucket["revenue"],
        runtime=movie_from_bucket["runtime"],
        overview=movie_from_bucket["overview"]
    )

    if movie.movie_id is None:
        raise TypeError("No movie id found")
    if movie.title is None:
        raise TypeError("No movie title found")
    
    # Use the schema to get a metadata dict
    # To get a metadata dict, remove pairs whose value is None
    # Also convert each list of strings into a string of CSV
    metadata: dict[str, Any] = {}
    for key, val in movie.model_dump(exclude=set(KEYS_NOT_IN_METADATA)).items():
        if val is not None:
            if isinstance(val, list):
                metadata[key] = ", ".join(cast(list[str], val))
            else:
                metadata[key] = val

    # Prepare parameters for adding embeddings to Chroma collection
    ids: list[str] = []
    documents: list[str] = []
    metadatas: list[Metadata] = []

    ids = [
        f"{movie.movie_id}-{CAST_KEY}",
        f"{movie.movie_id}-{DIRECTORS_KEY}",
        f"{movie.movie_id}-{OVERVIEW_KEY}",
    ]
    documents=[
        f"{CAST_KEY}: {getattr(movie, CAST_KEY)}",
        f"{DIRECTORS_KEY}: {getattr(movie, DIRECTORS_KEY)}",
        f"{OVERVIEW_KEY}: {getattr(movie, OVERVIEW_KEY)}"
    ]
    metadatas=[dict(metadata) for _ in range(len(SEMANTIC_KEYS))]

    # Add contextual embeddings to Chroma collection
    global chroma_collection
    chroma_collection.add(
        ids=ids,
        documents=documents,
        metadatas=metadatas
    )

def prepare_chroma_collection_sync() -> None:
    """
        Deletes the existing Chroma collection if it exists
        Creates a new empty Chroma collection
    """
    global chroma_collection

    # Delete Chroma collection
    try:
        chroma_client.delete_collection(CHROMA_COLLECTION_NAME)
    except Exception as e:
        if not isinstance(e, NotFoundError): 
            traceback.print_exc()
            return
        
    # Create new Chroma collection
    try:
        chroma_collection = chroma_client.create_collection(
            name=CHROMA_COLLECTION_NAME,
            embedding_function=openai_ef # type: ignore
        )
        print("New Chroma collection created.")
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

    # Set up Chroma collection for storing embeddings
    await asyncio.to_thread(prepare_chroma_collection_sync)
    
    print("Proceeding with data indexing pipeline...")
    indexing_tasks = await asyncio.to_thread(get_indexing_tasks_sync)
    indexing_tasks_results: list[BaseException | None] = await asyncio.gather(
        *indexing_tasks,
        return_exceptions=True
    )

    # Process results of the batched tasks
    num_results_processed = 0
    for res in indexing_tasks_results:
        if isinstance(res, BaseException):
            e = res
            print("Task failed:")
            print(e)
        else:
            num_results_processed += 1

    print(
        f"{num_results_processed} out of {len(indexing_tasks_results)}"
        " movies have been successfully indexed."
    )

    # Check the number of embeddings in the Chroma collection
    num_embeddings = await asyncio.to_thread(chroma_collection.count)
    print(f"There are {num_embeddings} embeddings in the Chroma collection.")

    end_time = time.time()
    print(f"Data indexing pipeline ran in {end_time - start_time} seconds")

async def get_prompt_section_movie_content(user_query: str) -> str:
    """
        Takes in the user input asking for a recommendation
        Queries ChromaDB for semantically similar content
        Formats the retrieval results into a string format for LLM
    """
    initial_query_results = await asyncio.to_thread(
        chroma_collection.query,
        query_texts=user_query,
        n_results=2
    )
    
    # Get a list of movie id's from the query results returned by Chroma
    relevant_movie_ids: list[int] = [
        cast(int, movie["movie_id"])
        for movie in cast(list[list[Metadata]],
                          initial_query_results["metadatas"])[0]
    ]

    # Remove duplicate movie id's
    relevant_movie_ids = list(set(relevant_movie_ids))

    # For each movie id, fetch all data for that movie from ChromaDB
    full_movie_results = await asyncio.to_thread(
        chroma_collection.get,
        where=cast(Where, {
            "movie_id": {"$in": relevant_movie_ids}
        }), # alternatively can form embedding id directly
    )

    # Prepare for parsing results
    embedding_ids = full_movie_results["ids"]
    # Cast is used because default type hint is an Optional
    metadatas = cast(list[Metadata], full_movie_results["metadatas"])
    documents = cast(list[str], full_movie_results["documents"])

    # Aggregates data for each movie id using the database results
    # This data will be converted to JSON to be sent to the LLM
    movie_instance_by_id: dict[int, MovieSchema] = {
        movie_id : MovieSchema() for movie_id in relevant_movie_ids
    }

    # Loop through the list of results
    for embedding_id, metadata, document in zip(embedding_ids, metadatas, documents):
        # Extract movie id from metadata
        movie_id = cast(int, metadata["movie_id"])
        if movie_id not in movie_instance_by_id:
            continue

        movie_instance: MovieSchema = movie_instance_by_id[movie_id]

        # If metadata info is not populated yet, populate metadata info 
        # Since "movie_id" is a required attribute found in the metadata dict,
        # if this attribute is not found in the to-be JSON object,
        # this indicates that the object has not yet been popululated with
        # metadata information
        if movie_instance.movie_id is None:
            # Create a combined info object with both existing info and new metadata
            current_info_dict = movie_instance.model_dump(exclude_unset=True)
            current_info_dict.update(metadata) # Now updated

            # Map the current movie_id to this new BaseModel instance
            # Correctly parses fields like "genres" using a custom validator function
            movie_instance_by_id[movie_id] = MovieSchema.model_validate(current_info_dict)
            
            # Ensure the variable movie_instance refers to the new
            # object associated with the current movie_id key 
            movie_instance = movie_instance_by_id[movie_id]
        
        # Check to see which key attribute the current embedding represents
        # Add non-metadata info like "overview" to the movie instance
        for key in KEYS_NOT_IN_METADATA:
            if embedding_id.endswith(key):
                setattr(movie_instance, key, document)
    
    # Convert each movie instance into a dict to get a list of dicts for JSON
    formatted_movies: list[dict[str, Any]] = [
        movie_instance.model_dump()
        for movie_instance in movie_instance_by_id.values()
    ]
    return json.dumps(formatted_movies, indent=2) # TODO: Make non-parse-able

# ENDPOINTS

@app.post("/admin/execute_data_pipeline")
async def execute_data_pipeline(payload: ExecuteDataPipelineRequest) -> DataPipelineResponse:
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
        return DataPipelineResponse(has_completed=True)
    except Exception:
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error has occurred."
        )

@app.post("/recommend")
async def generate_recommendation(payload: RecommendationPayload) -> Recommendation:
    """
        Main RAG endpoint for generating a movie recommendation
        Queries ChromaDB for semantically similar content to the user's query
        An LLM uses this context to output a natural language response
    """
    try:
        temp_response = payload.user_query.upper()

        prompt_section_movie_content = await get_prompt_section_movie_content(payload.user_query)
        print(prompt_section_movie_content)

        return Recommendation(recommendation=temp_response)
    except Exception as e:
        print(e)
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error has occurred."
        )

@app.get("/")
def root() -> dict[str, str]:
    return {"message": "API for Movie Guru Backend"}

@app.get("/status")
def get_status() -> dict[str, str]:
    return {"status": "ok"}