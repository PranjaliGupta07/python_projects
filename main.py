from fastapi import FastAPI, File, UploadFile, HTTPException
from minio import Minio
from minio.error import S3Error
from starlette.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from io import BytesIO

# MinIO configuration
MINIO_ENDPOINT = "127.0.0.1:9000"
MINIO_ACCESS_KEY = "4k9CYHyvT4wUDBHHUmwQ"
MINIO_SECRET_KEY = "L3kI2AIW8gymk4AsnMynyAsqxkDwdh7QcQljdQ0P"
MINIO_BUCKET_NAME = "fastdemo"

# Initialize MinIO client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False  # Set to True if using HTTPS
)

# Create FastAPI instance
app = FastAPI()

# CORS middleware configuration
origins = [
    "http://127.0.0.1:8000/",
    "http://localhost:8081",
    "http://localhost:8080",
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["DELETE", "GET", "POST", "PUT"],
    allow_headers=["*"],
)

# Create a bucket if it doesn't exist
try:
    if not minio_client.bucket_exists(MINIO_BUCKET_NAME):
        minio_client.make_bucket(MINIO_BUCKET_NAME)
except S3Error as err:
    print("Error occurred: ", err)

# Endpoint to upload a file
@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        # Read file data
        file_data = await file.read()
        
        # Convert bytes to BytesIO object
        file_obj = BytesIO(file_data)
        
        # Upload file to MinIO
        minio_client.put_object(
            MINIO_BUCKET_NAME,
            file.filename,
            data=file_obj,
            length=len(file_data),
            content_type=file.content_type
        )
        return {"message": f"File '{file.filename}' uploaded successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint to download a file
@app.get("/download/{filename}")
async def download_file(filename: str):
    try:
        # Get file from MinIO
        response = minio_client.get_object(MINIO_BUCKET_NAME, filename)
        content = response.read()
        response.close()
        response.release_conn()
        
        # Create a BytesIO object from the content
        return StreamingResponse(BytesIO(content), media_type="application/octet-stream")
    except S3Error as err:
        raise HTTPException(status_code=404, detail=str(err))

# Test endpoint for Postman
@app.get("/")
def read_root():
    return {"message": "This is Python FastAPI with MinIO"}

# Run the FastAPI application
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0",port=8000)