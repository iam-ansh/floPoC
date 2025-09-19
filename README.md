# FloatChat - Argo Float Data Explorer with LLaMA + Chroma + Gemini Embeddings

This project enables **retrieval-augmented generation (RAG)** over Argo float vertical profile data. It uses:

- **PostgreSQL** to store raw Argo data.
- **Google Gemini API** to generate embeddings for each row (chunk) of data.
- **ChromaDB (Docker)** as a vector store to persist embeddings.
- **LLaMA** to answer queries over the embedded Argo data.

---

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)

---

## Features

- Chunk Argo CSV / Postgres rows as individual documents.
- Generate embeddings using Google Gemini API.
- Store and retrieve embeddings from ChromaDB (Docker-based).
- Query row-level data with LLaMA.
- Metadata preservation for each row (timestamps, temperature, salinity, etc.).

---

## Architecture
1. **PostgreSQL** stores raw Argo float data.
2. Each row is converted into a **Document** for chunking.
3. **Gemini API** generates embeddings for each chunk.
4. **Chroma Docker server** persists embeddings and metadata.
5. **LLaMA** retrieves top-k chunks and generates natural language answers.

---
