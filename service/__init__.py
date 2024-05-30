from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
import os

host = os.environ.get("HOST", "192.168.1.57")
db_port = int(os.environ.get("DB_PORT", 30000))
username = os.environ.get("USERNAME", "gmr_api")
password = os.environ.get("PASSWORD", "Q1hTpYkpYNRzsUVs")


client = MongoClient(f"mongodb://{host}:{db_port}/")
db = client.gmrDB.get_collection("gmrdata")

