import os
import pandas as pd
from sqlalchemy import create_engine
from langchain.schema import Document
from langchain_chroma import Chroma
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from datetime import timedelta
from dotenv import load_dotenv
from chromadb.config import Settings


load_dotenv()

CHUNK_SIZE_DAYS = 5      
OVERLAP_DAYS = 2          
TABLE_NAME = "argo_data_test"
EMBED_MODEL = "models/embedding-001"


engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)


df = pd.read_sql(f"SELECT * FROM {TABLE_NAME} ORDER BY time_start ASC", engine)
df["time_start"] = pd.to_datetime(df["time_start"])

documents = []
if not df.empty:
    start_time = df["time_start"].iloc[0]

    while start_time <= df["time_start"].iloc[-1]:
        end_time = start_time + timedelta(days=CHUNK_SIZE_DAYS)
        chunk = df[(df["time_start"] >= start_time) & (df["time_start"] < end_time)]

        if not chunk.empty:
            text = chunk.to_csv(index=False)
            documents.append(
                Document(
                    page_content=text,
                    metadata={
                        "start": str(chunk["time_start"].iloc[0]),
                        "end": str(chunk["time_start"].iloc[-1]),
                    },
                )
            )
        start_time += timedelta(days=(CHUNK_SIZE_DAYS - OVERLAP_DAYS))

print(f"Created {len(documents)} overlapping time chunks")


chroma_settings = Settings(
    chroma_server_host=os.getenv("CHROMA_HOST"),
    chroma_server_http_port=int(os.getenv("CHROMA_PORT"))
)

embeddings = GoogleGenerativeAIEmbeddings(
    model="models/embedding-001",
    google_api_key=os.getenv("GOOGLE_API_KEY")
)


vectorstore = Chroma(
    collection_name="argo_collection_test",
    embedding_function=embeddings,
    client_settings=chroma_settings,
)


vectorstore.add_documents(documents)
print("Documents stored in remote Chroma DB!")
collection = vectorstore._client.get_collection("argo_collection_test")
print("Number of stored embeddings:", collection.count())

