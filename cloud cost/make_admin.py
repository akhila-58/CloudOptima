from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['cloudoptima']
result = db['users'].update_many({}, {'$set': {'is_admin': True}})
print(f"Modified {result.modified_count} users to be admins.")
