# Movie Guru Backend

This is the backend for the Movie Guru project, a full-stack web application that provides movie recommendations using a Retrieval-Augmented Generation (RAG) pipeline.

## About the Project
This backend is a **FastAPI** application that serves as the core of the Movie Guru project. It handles data ingestion, embedding generation, RAG retrieval from ChromaDB, and LLM-based recommendation generation.

## Features
- A robust data pipeline to ingest movie data from the **TMDb API** into **S3** and **ChromaDB**.
- A RAG system that leverages **ChromaDB** and **OpenAI's GPT-5 mini** to generate movie recommendations.
- A REST API with a `/recommend` endpoint that takes a user query and returns a recommendation.
- **Redis** caching to optimize the data ingestion pipeline.

## Technologies Used
- **Python**, **FastAPI**
- **ChromaDB**
- **OpenAI APIs (GPT-5 mini, Embedding API)**
- **AWS**
- **Redis**
- **TMDb API**
