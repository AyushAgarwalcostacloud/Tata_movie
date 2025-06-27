from fastapi import FastAPI
from es import should_search

app=FastAPI()

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/search/")
def search(input: str):
    results = should_search(input)
    return {"results": [results[i]['title'] for i in range(0, len(results))]}