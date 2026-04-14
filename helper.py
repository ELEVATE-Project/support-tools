import pandas as pd
import requests
import time, os

# ==== CONFIG ====
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.join(BASE_DIR,"test.xlsx" )  # your input excel file
OUTPUT_FILE = os.path.join(BASE_DIR,"output.xlsx" )

API_URL = "https://elevate-api.sunbirdsaas.com/entity-management/v1/entities/find"

HEADERS = {
    "internal-access-token": "",
    "content-type": "application/json",
    "x-auth-token": ""
}

# ==== READ EXCEL ====
df = pd.read_excel(INPUT_FILE)

# Make sure column names match your sheet
udise_col = "School UDISE code (11 digits)"
name_col = "School Name"

results = []

# ==== LOOP THROUGH ROWS ====
for index, row in df.iterrows():
    udise = str(row[udise_col]).strip()
    school_name = str(row[name_col]).strip()  # convert to lowercase for case-insensitive comparison
    print(f"Checking: {udise} - {school_name}...")
    payload = {
        "query": {
            "metaInformation.externalId": udise,
            "tenantId": "shikshagraha"
        }
    }
    print("Payload:", payload)  # Debug: print the payload being sent
    try:
        
        response = requests.post(API_URL, headers=HEADERS, json=payload)
        if response.status_code == 200:
            data = response.json()

            # Adjust this based on actual API response structure
            resultData = data.get("result") or data.get("data") or []

            if isinstance(resultData, list) and len(resultData) > 0:
                first_item = resultData[0]

                metaschoolname = first_item.get("metaInformation", {})
                api_school_name = metaschoolname.get("name", "").strip().lower()

                status = "Existing School"
            else:
                status = "School Not Found"
        else:
            status = f"Error {response.status_code}"

    except Exception as e:
        status = f"Exception: {str(e)}"

    print(f"{udise} - {school_name} -> {status}")

    results.append({
        "UDISE Code": udise,
        "School Name": school_name,
        "api_school_name": api_school_name,
        "Status": status
    })

    # avoid rate limiting
    time.sleep(0.2)

# ==== SAVE OUTPUT ====
output_df = pd.DataFrame(results)
output_df.to_excel(OUTPUT_FILE, index=False)

print("✅ Done! Output saved to", OUTPUT_FILE)