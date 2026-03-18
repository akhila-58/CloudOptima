from pymongo import MongoClient
from werkzeug.security import generate_password_hash

client = MongoClient('mongodb://localhost:27017/')
db = client['cloudoptima']

# 1. Remove admin rights from ram
db.users.update_one({'email': 'ram@gmail.com'}, {'$set': {'is_admin': False}})
print("Removed admin rights from ram@gmail.com")

# 2. Add or update rabiyamishkath@gmail.com
rabiya = db.users.find_one({'email': 'rabiyamishkath@gmail.com'})
if rabiya:
    db.users.update_one(
        {'email': 'rabiyamishkath@gmail.com'},
        {'$set': {'password': generate_password_hash('123456'), 'is_admin': True}}
    )
    print("Updated rabiyamishkath@gmail.com to admin")
else:
    db.users.insert_one({
        'username': 'rabiya',
        'email': 'rabiyamishkath@gmail.com',
        'password': generate_password_hash('123456'),
        'is_admin': True
    })
    print("Created admin account rabiyamishkath@gmail.com")

# 3. Add or update kottesoumyareddy@gmail.com
soumya = db.users.find_one({'email': 'kottesoumyareddy@gmail.com'})
if soumya:
    db.users.update_one(
        {'email': 'kottesoumyareddy@gmail.com'},
        {'$set': {'password': generate_password_hash('soumya21'), 'is_admin': True}}
    )
    print("Updated kottesoumyareddy@gmail.com to admin")
else:
    db.users.insert_one({
        'username': 'soumya',
        'email': 'kottesoumyareddy@gmail.com',
        'password': generate_password_hash('soumya21'),
        'is_admin': True
    })
    print("Created admin account kottesoumyareddy@gmail.com")

print("Done!")
