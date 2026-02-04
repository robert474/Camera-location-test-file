#!/usr/bin/env python3
"""
Traffic Camera Data Aggregator
Processes camera data from multiple sources into a standardized JSON format.
"""

import json
from datetime import datetime

def load_json(filepath):
    """Load JSON from file."""
    with open(filepath, 'r') as f:
        return json.load(f)

def process_nyc_dot(data):
    """Process NYC DOT camera data."""
    cameras = []
    for cam in data:
        cameras.append({
            "camera_id": cam.get("id", ""),
            "name": cam.get("name", ""),
            "latitude": cam.get("latitude"),
            "longitude": cam.get("longitude"),
            "image_url": cam.get("imageUrl", ""),
            "stream_url": None,
            "direction": None,
            "road": None,
            "status": "online" if cam.get("isOnline") == "true" else "offline",
            "raw_metadata": {
                "area": cam.get("area", "")
            }
        })
    return {
        "source_id": "ny_nycdot",
        "source_name": "NYC DOT",
        "source_url": "https://webcams.nyctmc.org/api/cameras",
        "state": "NY",
        "jurisdiction_type": "City",
        "last_fetched": datetime.utcnow().isoformat() + "Z",
        "api_type": "REST/JSON",
        "camera_count": len(cameras),
        "cameras": cameras
    }

def process_caltrans_d7(data):
    """Process Caltrans District 7 camera data."""
    cameras = []
    for item in data.get("data", []):
        cam = item.get("cctv", {})
        loc = cam.get("location", {})
        img = cam.get("imageData", {})
        static = img.get("static", {})

        cameras.append({
            "camera_id": cam.get("index", ""),
            "name": loc.get("locationName", ""),
            "latitude": float(loc.get("latitude", 0)) if loc.get("latitude") else None,
            "longitude": float(loc.get("longitude", 0)) if loc.get("longitude") else None,
            "image_url": static.get("currentImageURL", ""),
            "stream_url": img.get("streamingVideoURL"),
            "direction": loc.get("direction"),
            "road": loc.get("route"),
            "status": "online" if cam.get("inService") == "true" else "offline",
            "raw_metadata": {
                "district": loc.get("district"),
                "county": loc.get("county"),
                "nearbyPlace": loc.get("nearbyPlace"),
                "elevation": loc.get("elevation"),
                "postmile": loc.get("postmile"),
                "milepost": loc.get("milepost")
            }
        })
    return {
        "source_id": "ca_caltrans_d7",
        "source_name": "Caltrans District 7 - LA Metro",
        "source_url": "https://cwwp2.dot.ca.gov/data/d7/cctv/cctvStatusD07.json",
        "state": "CA",
        "jurisdiction_type": "State District",
        "last_fetched": datetime.utcnow().isoformat() + "Z",
        "api_type": "REST/JSON",
        "camera_count": len(cameras),
        "cameras": cameras
    }

def process_511ny(data):
    """Process 511NY camera data."""
    cameras = []
    cam_list = data.get("item2", [])
    for cam in cam_list:
        loc = cam.get("location", [])
        cameras.append({
            "camera_id": cam.get("itemId", ""),
            "name": cam.get("title", ""),
            "latitude": loc[0] if len(loc) > 0 else None,
            "longitude": loc[1] if len(loc) > 1 else None,
            "image_url": f"https://511ny.org/map/Ede/Cameras/{cam.get('itemId', '')}",
            "stream_url": None,
            "direction": None,
            "road": None,
            "status": "online",
            "raw_metadata": {}
        })
    return {
        "source_id": "ny_511ny",
        "source_name": "511NY - New York State",
        "source_url": "https://511ny.org/map/mapIcons/Cameras",
        "state": "NY",
        "jurisdiction_type": "State",
        "last_fetched": datetime.utcnow().isoformat() + "Z",
        "api_type": "REST/JSON",
        "camera_count": len(cameras),
        "cameras": cameras
    }

def process_fl511(data):
    """Process FL511 camera data."""
    cameras = []
    cam_list = data.get("item2", [])
    for cam in cam_list:
        loc = cam.get("location", [])
        expando = cam.get("expando", {})
        cameras.append({
            "camera_id": cam.get("itemId", ""),
            "name": cam.get("title", ""),
            "latitude": loc[0] if len(loc) > 0 else None,
            "longitude": loc[1] if len(loc) > 1 else None,
            "image_url": f"https://fl511.com/map/Cameras/{cam.get('itemId', '')}",
            "stream_url": None,
            "direction": None,
            "road": None,
            "status": "online",
            "raw_metadata": {
                "videoEnabled": expando.get("videoEnabled", False)
            }
        })
    return {
        "source_id": "fl_fl511",
        "source_name": "FL511 - Florida",
        "source_url": "https://fl511.com/map/mapIcons/Cameras",
        "state": "FL",
        "jurisdiction_type": "State",
        "last_fetched": datetime.utcnow().isoformat() + "Z",
        "api_type": "REST/JSON",
        "camera_count": len(cameras),
        "cameras": cameras
    }

def main():
    """Main function to aggregate all camera data."""
    sources = []
    total_cameras = 0
    sources_processed = 0
    sources_failed = 0

    # Process NYC DOT
    try:
        nyc_data = load_json("/tmp/nyc_cameras.json")
        nyc_source = process_nyc_dot(nyc_data)
        sources.append(nyc_source)
        total_cameras += nyc_source["camera_count"]
        sources_processed += 1
        print(f"NYC DOT: {nyc_source['camera_count']} cameras")
    except Exception as e:
        print(f"Failed to process NYC DOT: {e}")
        sources_failed += 1

    # Process Caltrans D7
    try:
        caltrans_data = load_json("/tmp/caltrans_d7.json")
        caltrans_source = process_caltrans_d7(caltrans_data)
        sources.append(caltrans_source)
        total_cameras += caltrans_source["camera_count"]
        sources_processed += 1
        print(f"Caltrans D7: {caltrans_source['camera_count']} cameras")
    except Exception as e:
        print(f"Failed to process Caltrans D7: {e}")
        sources_failed += 1

    # Process 511NY
    try:
        ny511_data = load_json("/tmp/511ny_cameras.json")
        ny511_source = process_511ny(ny511_data)
        sources.append(ny511_source)
        total_cameras += ny511_source["camera_count"]
        sources_processed += 1
        print(f"511NY: {ny511_source['camera_count']} cameras")
    except Exception as e:
        print(f"Failed to process 511NY: {e}")
        sources_failed += 1

    # Process FL511
    try:
        fl511_data = load_json("/tmp/fl511_cameras.json")
        fl511_source = process_fl511(fl511_data)
        sources.append(fl511_source)
        total_cameras += fl511_source["camera_count"]
        sources_processed += 1
        print(f"FL511: {fl511_source['camera_count']} cameras")
    except Exception as e:
        print(f"Failed to process FL511: {e}")
        sources_failed += 1

    # Create aggregated output
    output = {
        "metadata": {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "total_cameras": total_cameras,
            "sources_processed": sources_processed,
            "sources_failed": sources_failed,
            "notes": "Initial batch of 4 sources for format verification. Illinois Gateway requires registration."
        },
        "sources": sources
    }

    # Write output file
    output_path = "/Users/robert/Desktop/getargus_projects/Camera location test file/traffic_cameras_aggregated.json"
    with open(output_path, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\nAggregated {total_cameras} cameras from {sources_processed} sources")
    print(f"Output written to: {output_path}")

if __name__ == "__main__":
    main()
