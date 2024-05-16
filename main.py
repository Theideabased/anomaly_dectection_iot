from fastapi import FastAPI, UploadFile, File, HTTPException
from io import BytesIO
import pandas as pd
import joblib  

# Load the pickled model
model = joblib.load("model/my_model.pkl")

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "the anomalies dectection app"}

async def process_csv(csv_file: UploadFile = File(...)):
    # Read the uploaded CSV file
    try:
        if hasattr(csv_file, 'file'):
            data = pd.read_csv(csv_file.file)
        else:
            content = await csv_file.read() 
            data = pd.read_csv(BytesIO(content))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading CSV file: {str(e)}")

    # Check if "source" column exists
    if "Destination" not in data.columns:
        raise HTTPException(status_code=400, detail="CSV file is missing the 'source' column")

    # Extract the source column for predictions
    source_data = data["Destination"]

    # Make predictions using the loaded model
    predictions = model.predict(source_data)

    # Add a new column named "predicted" to the DataFrame
    data["predicted"] = predictions

    # Return the processed DataFrame as a dictionary
    return data.to_dict(orient="records")


@app.post("/predict")
async def predict(csv_file: UploadFile = File(...)):
    # Process the CSV and make predictions
    try:
        processed_data = await process_csv(csv_file)
        return processed_data
    except HTTPException as e:
        return e

