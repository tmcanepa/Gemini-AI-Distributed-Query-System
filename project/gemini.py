import google.generativeai as genai
import os
from dotenv import load_dotenv

load_dotenv()

gemini_api = os.getenv("GEMINI_API_KEY")

genai.configure(api_key=gemini_api)
model = genai.GenerativeModel("gemini-1.5-flash")
response = model.generate_content("What about a smaller animal than that?")
print(response.text)