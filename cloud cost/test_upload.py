import requests
import io
import time

s = requests.Session()
# Login as admin to get the session cookie
login_url = "http://127.0.0.1:5000/login"
r = s.post(login_url, data={'email': 'admin@cloudoptima.com', 'password': 'password'})
print(f"Login Status: {r.status_code}")

# Upload a mock CSV
csv_data = "Resource_ID,Resource_Type,Region,Status,Cost,Usage_Hours\n1,EC2,us-east,running,100,500\n2,RDS,us-west,stopped,50,0"
files = {'file': ('test_report.csv', io.StringIO(csv_data), 'text/csv')}

upload_url = "http://127.0.0.1:5000/upload"
r_upload = s.post(upload_url, files=files)
print(f"Upload Status: {r_upload.status_code}")

# Trigger the export route to see if it responds with the proper Header name
download_url = "http://127.0.0.1:5000/download_report"
r_download = s.get(download_url)
print(f"Download Header: {r_download.headers.get('Content-disposition')}")
