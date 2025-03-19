from pymongo import MongoClient
import pandas as pd
import os
from dotenv import load_dotenv

from pathlib import Path

env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)
# Debugging için ortam değişkenlerini ekrana yazdır
print("MongoDB URI:", os.getenv("MONGODB_URI"))
print("MongoDB DB:", os.getenv("MONGODB_DB"))
print("MongoDB Collection:", os.getenv("MONGODB_COLLECTION"))

def fetch_and_export():
    # Ortam değişkenlerini al
    uri = os.getenv("MONGODB_URI")
    db_name = os.getenv("MONGODB_DB")
    collection_name = os.getenv("MONGODB_COLLECTION")

    # MongoDB'ye bağlan
    client = MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]

    # Verileri çek
    cursor = collection.find({})
    df = pd.DataFrame(list(cursor))

    # MongoDB'nin _id alanını kaldır (CSV'de gereksiz)
    if "_id" in df.columns:
        df.drop(columns=["_id"], inplace=True)

    # CSV olarak kaydet
    output_path = "data/raw/ifadeler.csv"
    df.to_csv(output_path, index=False)

    print(f"Veriler başarıyla {output_path} dosyasına aktarıldı!")

if __name__ == "__main__":
    fetch_and_export()
