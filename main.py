from fastapi import FastAPI

app = FastAPI()

@app.get("/")
async def root():
    return {'message': 'API for Movie Guru Backend'}

@app.get("/status")
async def get_status():
    return {"status": "ok"}