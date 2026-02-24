import OpenDartReader
import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("DART_API_KEY")

try:
    print("Initializing OpenDartReader...")
    dart = OpenDartReader(api_key)
    print("Success!")
    # Test a simple call
    res = dart.find_corp_code('005930')
    print(f"Corp code for Samsung: {res}")
except Exception as e:
    import traceback
    traceback.print_exc()
