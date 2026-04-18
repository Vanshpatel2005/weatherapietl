import requests
import json

resp = requests.get('http://localhost:8000/api/aggregated-metrics?days=7')
data = resp.json()
if data['success']:
    if data['data']:
        first = data['data'][0]
        print(json.dumps(first, indent=2))
        
        # Check for any missing expected fields
        expected = ["aggregation_date", "avg_temperature", "min_temperature", "max_temperature", "avg_humidity", "avg_wind_speed", "dominant_weather_main", "data_quality_score"]
        for field in expected:
            if field not in first:
                print(f"MISSING FIELD: {field}")
    else:
        print("No records returned")
else:
    print("API Error")
