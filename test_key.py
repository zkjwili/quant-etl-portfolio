from dotenv import load_dotenv
import os

load_dotenv()
key = os.getenv("FRED_API_KEY")

if key:
    print(f"SUCCESS! Python found your key: {key[:5]}...{key[-5:]}")
else:
    print("FAILED! Python cannot find your key. Check your .env file.")
