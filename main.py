from dotenv import load_dotenv
from fastapi import FastAPI
import os
import tmdbsimple as tmdb # type: ignore
from typing import Any

load_dotenv(dotenv_path=".env.development")
app = FastAPI()

NUM_MOVIES = 100 # TBD change to 10000
tmdb.API_KEY = os.getenv("TMDB_API_KEY")

def load_movies():
    num_pages = (NUM_MOVIES + 19) // 20
    movie = tmdb.Movies()

    movie_list: Any = []
    for i in range(num_pages):
        try:
            response = movie.popular(page=(i + 1)) # type: ignore
            movie_list.extend(response["results"])

            # TODO migrate to S3
        except Exception as e:
            print(e)
            break
    
    num_movies_loaded = len(movie_list)
    print(f"A total of {num_movies_loaded} movies have been loaded.")
    return num_movies_loaded

load_movies()

@app.get("/")
async def root():
    return {"message": "API for Movie Guru Backend"}

@app.get("/status")
async def get_status():
    return {"status": "ok"}