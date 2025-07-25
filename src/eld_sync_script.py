import requests
from datetime import datetime
import os


# config
SPARKLE_API_KEY = os.getenv("SPARKLE_API_KEY")
HUBSPOT_API_TOKEN = os.getenv("HUBSPOT_API_TOKEN")
CUSTOM_OBJECT_NAME = "eld_sync"
CUSTOM_OBJECT_LABELS = {
    "singular": "ELD Sync",
    "plural": "ELD Syncs"
}

sparkle_headers = {
    'X-Api-Key': SPARKLE_API_KEY,
}
hubspot_headers = {
    "Authorization": f"Bearer {HUBSPOT_API_TOKEN}",
    "Content-Type": "application/json"
}
# --------------------------------


# get or create hubspot custom object
def get_or_create_custom_object():
    # Step 1: Try to fetch existing custom schemas
    schemas_url = "https://api.hubapi.com/crm/v3/schemas"
    response = requests.get(schemas_url, headers=hubspot_headers)

    if response.status_code != 200:
        print(f"❌ Failed to fetch schemas: {response.status_code}")
        print(response.text)
        return None

    for schema in response.json().get("results", []):
        if schema.get("name") == CUSTOM_OBJECT_NAME:
            print(f"✅ Found existing custom object: {CUSTOM_OBJECT_NAME}")
            return schema["objectTypeId"]

    # Step 2: If not found, create it
    print(f"ℹ️ Custom object '{CUSTOM_OBJECT_NAME}' not found. Creating...")
    payload = {
        "name": CUSTOM_OBJECT_NAME,
        "labels": CUSTOM_OBJECT_LABELS,
        "primaryDisplayProperty": "unit_id",
        "requiredProperties": ["unit_id"],
        "properties": [
            { "name": "unit_id", "label": "Unit ID", "type": "string", "fieldType": "text" },
            { "name": "driver_eld", "label": "Driver", "type": "string", "fieldType": "text" },
            { "name": "location_eld", "label": "Location", "type": "string", "fieldType": "text" },
            { "name": "engine_hours", "label": "Engine Hours", "type": "number", "fieldType": "number" },
            { "name": "mileage", "label": "Mileage", "type": "number", "fieldType": "number" },
            { "name": "last_sync_logs", "label": "Last Sync", "type": "string", "fieldType": "text" },
            { "name": "eld_serial_no", "label": "Serial Number", "type": "string", "fieldType": "text" },
            { "name": "eld_status", "label": "Status", "type": "string", "fieldType": "text" }
        ]
    }

    create_response = requests.post(schemas_url, headers=hubspot_headers, json=payload)

    if create_response.status_code == 201:
        object_type_id = create_response.json()["objectTypeId"]
        print(f"✅ Created custom object: {object_type_id}")
        return object_type_id
    else:
        print(f"❌ Failed to create custom object: {create_response.status_code}")
        print(create_response.text)
        return None

# fetch sparkel data
def fetch_sparkle_data():
    driversEndpoint = 'https://web.sparkleeld.us/api/v0/driverProfiles'
    vehicleLocationEndpoint = 'https://web.sparkleeld.us/api/v0/locations'
    eldDevicesEndpoint = 'https://web.sparkleeld.us/api/v0/devices/eld'

    # 1. Drivers
    drivers_dict = {}
    params_drivers = {
        'page': 1,
        'elements': 100,
        'asc': True,
        'orderBy': 'driverName',
        'quickSearch': '',
        'violationStatus': '',
    }
    r = requests.get(driversEndpoint, headers=sparkle_headers, params=params_drivers)
    if r.status_code == 200:
        drivers = r.json().get('data', [])
        drivers_dict = {d.get('vehicleId'): d for d in drivers if d.get('vehicleId')}
    else:
        print("❌ Drivers fetch failed.")

    # 2. Locations
    vehicle_locations_dict = {}
    r = requests.get(vehicleLocationEndpoint, headers=sparkle_headers)
    if r.status_code == 200:
        locations = r.json().get('vehicles', [])
        vehicle_locations_dict = {v.get('vehicleId'): v for v in locations if v.get('vehicleId')}
    else:
        print("❌ Locations fetch failed.")

    # 3. ELD devices (across all statuses)
    eld_devices_dict = {}
    statuses = ['active', 'deactivated', 'onHold', 'replaced']
    for status in statuses:
        page = 1
        while True:
            params = {
                'page': page,
                'elements': 100,
                'asc': True,
                'orderBy': 'vehicleId',
                'status': status
            }
            r = requests.get(eldDevicesEndpoint, headers=sparkle_headers, params=params)
            if r.status_code != 200:
                break
            data = r.json()
            content = data.get('content') or data.get('data') or data.get('items') or []
            for item in content:
                vid = item.get('vehicleId')
                if vid:
                    eld_devices_dict[vid] = item
            if data.get("last") or len(content) < 100:
                break
            page += 1

    # Merge data
    all_ids = set(drivers_dict.keys()) | set(vehicle_locations_dict.keys()) | set(eld_devices_dict.keys())
    combined = []
    for vid in all_ids:
        combined.append({
            "vehicleId": vid,
            "driverProfile": drivers_dict.get(vid),
            "vehicleLocation": vehicle_locations_dict.get(vid),
            "eldDevice": eld_devices_dict.get(vid)
        })
    return combined

# to hubSpot jsonFormat
def transform_data(combined_data):
    flattened = []
    for entry in combined_data:
        vehicle_id = entry.get("vehicleId")
        driver = entry.get("driverProfile") or {}
        location = entry.get("vehicleLocation") or {}
        eld = entry.get("eldDevice") or {}

        flattened.append({
            "unit_id": vehicle_id,
            "driver__eld_": driver.get("driverName"),
            "location__eld_": location.get("geoCodedLocation"),
            "engine_hours": location.get("engineHours"),
            "mileage": location.get("odometer"),
            "last_sync__logs_": datetime.now().strftime("%d.%m.%Y %H:%M"),
            "eld_serial_no_": eld.get("serialNum"),
            "eld_status": eld.get("status")
        })
    return flattened

# to hubSpot
def push_to_hubspot(object_type_id, transformed_data):
    def get_record_id_by_unit_id(unit_id):
        search_url = f"https://api.hubapi.com/crm/v3/objects/{object_type_id}/search"
        payload = {
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": "unit_id",
                            "operator": "EQ",
                            "value": unit_id
                        }
                    ]
                }
            ],
            "properties": ["unit_id"],
            "limit": 1
        }

        res = requests.post(search_url, headers=hubspot_headers, json=payload)
        if res.status_code == 200:
            results = res.json().get("results", [])
            if results:
                return results[0]["id"]
        return None

    for record in transformed_data:
        record_id = get_record_id_by_unit_id(record["unit_id"])
        payload = {
            "properties": {
                "unit_id": record["unit_id"],
                "driver_eld": record["driver__eld_"],
                "location_eld": record["location__eld_"],
                "engine_hours": record["engine_hours"],
                "mileage": record["mileage"],
                "last_sync_logs": record["last_sync__logs_"],
                "eld_serial_no": record["eld_serial_no_"],
                "eld_status": record["eld_status"]
            }
        }

        if record_id:
            url = f"https://api.hubapi.com/crm/v3/objects/{object_type_id}/{record_id}"
            res = requests.patch(url, headers=hubspot_headers, json=payload)
            action = "Updated"
        else:
            url = f"https://api.hubapi.com/crm/v3/objects/{object_type_id}"
            res = requests.post(url, headers=hubspot_headers, json=payload)
            action = "Created"

        if res.status_code in [200, 201]:
            print(f"✅ {action} ELD record: {record['unit_id']}")
        else:
            print(f"❌ Failed to {action.lower()} record: {record['unit_id']}")
            print(res.text)

# main
object_type_id = get_or_create_custom_object()

if object_type_id:
    combined_data = fetch_sparkle_data()
    transformed_data = transform_data(combined_data)
    
    push_to_hubspot(object_type_id, transformed_data)
else:
    print("❌ Aborting: Could not get objectTypeId")
