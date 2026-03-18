from pymongo import MongoClient
import datetime
client = MongoClient('mongodb://localhost:27017/')
db = client['cloudoptima']
result = db.resource_data.update_many(
    {'source_file': {'$exists': False}}, 
    {'$set': {'source_file': 'Legacy Admin File.csv', 'upload_time': datetime.datetime.now()}}
)
print(f"Patched {result.modified_count} records.")
