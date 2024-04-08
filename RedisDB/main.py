import redis
from RedisDB.RedisDB import RedisDB

def main():
    # Step 1) create RedisDB Object
    uploader = RedisDB()
    csv_path = "Data/heart_2020_cleaned.csv"

    # Step 2) Upload csv data to Redis
    try:
        uploader.redis_client.ping()
        print("Connection to Redis successful!")
        success = uploader.upload_csv_to_redis(csv_path)
        if success:
            print("CSV data uploaded to Redis successfully!")
        else:
            print("Failed to upload CSV data to Redis.")
    except redis.ConnectionError as e:
        print(f"Unable to connect to Redis: {e}")

if __name__ == "__main__":
    main()




