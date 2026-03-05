from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api import datasource, task, audit, data_management

app = FastAPI(title="Data Preprocessing System API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify the frontend origin
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(datasource.router)
app.include_router(task.router)
app.include_router(audit.router)
app.include_router(data_management.router)

@app.get("/")
def read_root():
    return {"message": "Welcome to Data Preprocessing System API"}
