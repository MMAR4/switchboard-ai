import os
from databricks import sql
from dotenv import load_dotenv

# Load credentials from .env
load_dotenv()

def test_connection():
    hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    token = os.getenv("DATABRICKS_TOKEN")

    if not all([hostname, http_path, token]):
        print("❌ Error: Missing credentials in .env file!")
        return

    print("Connecting to live Databricks SQL Warehouse...")
    try:
        with sql.connect(
            server_hostname=hostname,
            http_path=http_path,
            access_token=token
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 'Switchboard Live Connection Successful!' AS status")
                result = cursor.fetchall()
                print("✅ Response from Databricks:", result[0][0])
    except Exception as e:
        print("❌ Connection Failed:")
        print(e)

if __name__ == "__main__":
    test_connection()