
from datetime import datetime
from typing import Union
from fastapi import FastAPI
from fastapi import File, UploadFile
from PIL import Image
import pytesseract
import os
import time
import re

app = FastAPI()

 
@app.post("/upload")
def upload(file: UploadFile = File(...)):
    try:
        # Get the current working directory
        current_dir = os.getcwd()
        print(f"Current working directory: {current_dir}")

        # Create 'uploads' folder if it doesn't exist
        upload_dir = os.path.join(current_dir, "uploads")
        print(f"Upload directory path: {upload_dir}")

        os.makedirs(upload_dir, exist_ok=True)

        # Generate filename using current datetime
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_extension = os.path.splitext(file.filename)[1]
        new_filename = f"{current_time}{file_extension}"

        # Construct the full file path
        file_path = os.path.join(upload_dir, new_filename)
        print(f"Full file path: {file_path}")

        # Read and write the file contents
        contents = file.file.read()
        with open(file_path, 'wb') as f:
            f.write(contents)
            
       
        image = Image.open(file_path )
        extracted_text = pytesseract.image_to_string(image, config='--psm 6')
        result = extract_text(extracted_text)
        delete_file(file_path)    

        # Check if file was created
        if os.path.exists(file_path):
            print(f"File successfully created at: {file_path}")
        else:
            print(f"File creation failed. Path does not exist: {file_path}")

        return  {"message1": f"Successfully uploaded {new_filename}", "message2":extracted_text , "message3":result }

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return  {"error": f"There was an error uploading the file: {str(e)}"}

    finally:
        file.file.close()
        

        
@app.get("/")
def read_root():
    return {"Welcome": "to my first API"}

@app.get("/show")
async def read_random_file():
    file = os.listdir()
    
    
@app.get("/img2text/{item_id}")
def read_item(item_id: int, url: Union[str, None] = None):
    return {"text":  url}

@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}

import re

def extract_text(input_text):
    # Define the regex pattern to find '81' followed by exactly 8 digits
    pattern = r'81\d{8}'

    # Search the input text for the pattern
    match = re.search(pattern, input_text)

    # If a match is found, return it; otherwise, return an empty string or None
    if match:
        return match.group()  # This will return the entire match
    else:
        return "No match found"

def delete_file(filename: str):
    file_path = os.path.join(filename)
    if os.path.exists(file_path):
        os.remove(file_path)  # Delete the file 
 