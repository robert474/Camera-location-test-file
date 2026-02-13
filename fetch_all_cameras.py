#!/usr/bin/env python3
"""
Traffic Camera Data Aggregator - Batch Processor
Automatically fetches camera data from all known sources.

Last updated: 2026-02-04
Working sources: 27
Total cameras: ~23,000+
"""

import json
import requests
import time
from datetime import datetime, timezone
import sys

# Disable SSL warnings for some older government sites
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration
OUTPUT_FILE = "traffic_cameras_aggregated.json"
TIMEOUT = 30
DELAY_BETWEEN_REQUESTS = 0.5  # Be nice to servers

# ============================================================================
# VERIFIED WORKING SOURCES
# These have been tested and confirmed to return camera data
# ============================================================================

SOURCES = [
    # === NYC Area ===
    {"id": "ny_nycdot", "name": "NYC DOT", "state": "NY", "type": "City",
     "url": "https://webcams.nyctmc.org/api/cameras", "processor": "nyc_dot"},

    # === 511 Systems (confirmed working) ===
    {"id": "ny_511ny", "name": "511NY - New York State", "state": "NY", "type": "State",
     "url": "https://511ny.org/map/mapIcons/Cameras", "processor": "511_system"},
    {"id": "fl_fl511", "name": "FL511", "state": "FL", "type": "State",
     "url": "https://fl511.com/map/mapIcons/Cameras", "processor": "511_system"},
    {"id": "ga_511ga", "name": "Georgia 511", "state": "GA", "type": "State",
     "url": "https://511ga.org/map/mapIcons/Cameras", "processor": "511_system"},
    {"id": "az_az511", "name": "Arizona 511", "state": "AZ", "type": "State",
     "url": "https://az511.gov/map/mapIcons/Cameras", "processor": "511_system"},
    {"id": "la_511la", "name": "Louisiana 511", "state": "LA", "type": "State",
     "url": "https://511la.org/map/mapIcons/Cameras", "processor": "511_system"},
    {"id": "nv_nvroads", "name": "Nevada Roads", "state": "NV", "type": "State",
     "url": "https://nvroads.com/map/mapIcons/Cameras", "processor": "511_system"},
    {"id": "ut_udot", "name": "Utah DOT", "state": "UT", "type": "State",
     "url": "https://udottraffic.utah.gov/map/mapIcons/Cameras", "processor": "511_system"},
    {"id": "wi_511wi", "name": "Wisconsin 511", "state": "WI", "type": "State",
     "url": "https://511wi.gov/map/mapIcons/Cameras", "processor": "511_system"},
    {"id": "pa_511pa", "name": "Pennsylvania 511", "state": "PA", "type": "State",
     "url": "https://511pa.com/map/mapIcons/Cameras", "processor": "511_system"},
    {"id": "ct_cttravel", "name": "Connecticut Travel", "state": "CT", "type": "State",
     "url": "https://ctroads.org/map/mapIcons/Cameras", "processor": "511_system"},
    {"id": "ne_newengland511", "name": "New England 511 (ME/VT/NH)", "state": "ME/VT/NH", "type": "Regional",
     "url": "https://newengland511.org/map/mapIcons/Cameras", "processor": "511_system"},
    {"id": "id_511id", "name": "Idaho 511", "state": "ID", "type": "State",
     "url": "https://511.idaho.gov/map/mapIcons/Cameras", "processor": "511_system"},
    {"id": "ak_511ak", "name": "Alaska 511", "state": "AK", "type": "State",
     "url": "http://511.alaska.gov/map/mapIcons/Cameras", "processor": "511_system"},

    # === Caltrans Districts (all 12 confirmed working) ===
    {"id": "ca_caltrans_d1", "name": "Caltrans District 1", "state": "CA", "type": "State District",
     "url": "https://cwwp2.dot.ca.gov/data/d1/cctv/cctvStatusD01.json", "processor": "caltrans"},
    {"id": "ca_caltrans_d2", "name": "Caltrans District 2", "state": "CA", "type": "State District",
     "url": "https://cwwp2.dot.ca.gov/data/d2/cctv/cctvStatusD02.json", "processor": "caltrans"},
    {"id": "ca_caltrans_d3", "name": "Caltrans District 3", "state": "CA", "type": "State District",
     "url": "https://cwwp2.dot.ca.gov/data/d3/cctv/cctvStatusD03.json", "processor": "caltrans"},
    {"id": "ca_caltrans_d4", "name": "Caltrans District 4 - Bay Area", "state": "CA", "type": "State District",
     "url": "https://cwwp2.dot.ca.gov/data/d4/cctv/cctvStatusD04.json", "processor": "caltrans"},
    {"id": "ca_caltrans_d5", "name": "Caltrans District 5", "state": "CA", "type": "State District",
     "url": "https://cwwp2.dot.ca.gov/data/d5/cctv/cctvStatusD05.json", "processor": "caltrans"},
    {"id": "ca_caltrans_d6", "name": "Caltrans District 6", "state": "CA", "type": "State District",
     "url": "https://cwwp2.dot.ca.gov/data/d6/cctv/cctvStatusD06.json", "processor": "caltrans"},
    {"id": "ca_caltrans_d7", "name": "Caltrans District 7 - LA Metro", "state": "CA", "type": "State District",
     "url": "https://cwwp2.dot.ca.gov/data/d7/cctv/cctvStatusD07.json", "processor": "caltrans"},
    {"id": "ca_caltrans_d8", "name": "Caltrans District 8", "state": "CA", "type": "State District",
     "url": "https://cwwp2.dot.ca.gov/data/d8/cctv/cctvStatusD08.json", "processor": "caltrans"},
    {"id": "ca_caltrans_d9", "name": "Caltrans District 9", "state": "CA", "type": "State District",
     "url": "https://cwwp2.dot.ca.gov/data/d9/cctv/cctvStatusD09.json", "processor": "caltrans"},
    {"id": "ca_caltrans_d10", "name": "Caltrans District 10", "state": "CA", "type": "State District",
     "url": "https://cwwp2.dot.ca.gov/data/d10/cctv/cctvStatusD10.json", "processor": "caltrans"},
    {"id": "ca_caltrans_d11", "name": "Caltrans District 11 - San Diego", "state": "CA", "type": "State District",
     "url": "https://cwwp2.dot.ca.gov/data/d11/cctv/cctvStatusD11.json", "processor": "caltrans"},
    {"id": "ca_caltrans_d12", "name": "Caltrans District 12", "state": "CA", "type": "State District",
     "url": "https://cwwp2.dot.ca.gov/data/d12/cctv/cctvStatusD12.json", "processor": "caltrans"},

    # === ArcGIS Sources ===
    {"id": "md_chart", "name": "Maryland CHART", "state": "MD", "type": "State",
     "url": "https://mdgeodata.md.gov/imap/rest/services/Transportation/MD_TrafficCameras/MapServer/0/query?where=1%3D1&outFields=*&f=json", "processor": "arcgis"},
]

# ============================================================================
# SOURCES THAT REQUIRE AUTHENTICATION OR REGISTRATION
# These are documented but not fetched automatically
# ============================================================================

REQUIRES_AUTH = [
    {"state": "CO", "name": "Colorado COTRIP", "url": "https://www.cotrip.org/api/graphql",
     "notes": "Uses GraphQL with specific queries, may require session"},
    {"state": "OH", "name": "Ohio OHGO", "url": "https://publicapi.ohgo.com/api/v1/Cameras",
     "notes": "Requires API key - register at https://publicapi.ohgo.com"},
    {"state": "IL", "name": "Travel Midwest / Illinois Gateway", "url": "https://travelmidwest.com/lmiga/",
     "notes": "Requires registration at https://travelmidwest.com/lmiga/registration.jsp"},
    {"state": "VA", "name": "Virginia 511", "url": "https://www.511virginia.org/",
     "notes": "May require session/cookies from browser"},
    {"state": "TN", "name": "Tennessee SmartWay", "url": "https://smartway.tn.gov/",
     "notes": "Angular app with protected API"},
    {"state": "TX", "name": "Texas DriveTexas", "url": "https://drivetexas.org/",
     "notes": "API requires authentication"},
    {"state": "MA", "name": "Massachusetts 511", "url": "https://mass511.com/",
     "notes": "Protected API, may need session"},
    {"state": "MN", "name": "Minnesota 511", "url": "https://511mn.org/",
     "notes": "Protected API"},
    {"state": "MI", "name": "Michigan DOT", "url": "https://mdotjboss.state.mi.us/",
     "notes": "Protected API"},
    {"state": "NJ", "name": "New Jersey 511", "url": "https://www.511nj.org/",
     "notes": "Returns 403 - requires authentication"},
]

# ============================================================================
# PROCESSORS
# ============================================================================

def fetch_url(url, timeout=TIMEOUT):
    """Fetch URL with error handling."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; TrafficCameraAggregator/1.0)',
        'Accept': 'application/json, text/plain, */*'
    }
    try:
        response = requests.get(url, headers=headers, timeout=timeout, verify=False)
        response.raise_for_status()
        return response.text
    except Exception as e:
        return None

def process_nyc_dot(data, source):
    """Process NYC DOT format."""
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
            "raw_metadata": {"area": cam.get("area", "")}
        })
    return cameras

def process_511_system(data, source):
    """Process 511 system format (item1=icon, item2=cameras array)."""
    cameras = []
    cam_list = data.get("item2", [])
    base_url = source["url"].rsplit("/map/", 1)[0]
    for cam in cam_list:
        loc = cam.get("location", [])
        expando = cam.get("expando", {})
        video_url = (expando.get("videoUrl") or expando.get("VideoUrl")
                     or cam.get("videoUrl") or cam.get("VideoUrl"))
        cameras.append({
            "camera_id": str(cam.get("itemId", "")),
            "name": cam.get("title", ""),
            "latitude": loc[0] if len(loc) > 0 else None,
            "longitude": loc[1] if len(loc) > 1 else None,
            "image_url": f"{base_url}/map/Cameras/{cam.get('itemId', '')}",
            "stream_url": video_url,
            "direction": None,
            "road": None,
            "status": "online",
            "raw_metadata": {"videoEnabled": expando.get("videoEnabled", False)} if expando else {}
        })
    return cameras

def process_caltrans(data, source):
    """Process Caltrans format."""
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
    return cameras

def process_arcgis(data, source):
    """Process ArcGIS REST API format."""
    cameras = []
    for feature in data.get("features", []):
        attrs = feature.get("attributes", {})
        geom = feature.get("geometry", {})
        cameras.append({
            "camera_id": str(attrs.get("OBJECTID", attrs.get("feedID", ""))),
            "name": attrs.get("location", attrs.get("name", "")),
            "latitude": attrs.get("lat", geom.get("y")),
            "longitude": attrs.get("long", geom.get("x")),
            "image_url": attrs.get("url", ""),
            "stream_url": None,
            "direction": None,
            "road": None,
            "status": "online",
            "raw_metadata": {
                "county": attrs.get("county"),
                "feedID": attrs.get("feedID")
            }
        })
    return cameras

PROCESSORS = {
    "nyc_dot": process_nyc_dot,
    "511_system": process_511_system,
    "caltrans": process_caltrans,
    "arcgis": process_arcgis,
}

def process_source(source):
    """Process a single source."""
    print(f"  Fetching {source['name']}...", end=" ", flush=True)

    text = fetch_url(source["url"])
    if not text:
        print("FAILED (fetch error)")
        return None

    try:
        processor = PROCESSORS.get(source["processor"])
        data = json.loads(text)
        cameras = processor(data, source)

        if not cameras:
            print("FAILED (no cameras found)")
            return None

        result = {
            "source_id": source["id"],
            "source_name": source["name"],
            "source_url": source["url"],
            "state": source["state"],
            "jurisdiction_type": source["type"],
            "last_fetched": datetime.now(timezone.utc).isoformat(),
            "api_type": "REST/JSON",
            "camera_count": len(cameras),
            "cameras": cameras
        }
        print(f"OK ({len(cameras)} cameras)")
        return result
    except Exception as e:
        print(f"FAILED ({str(e)[:50]})")
        return None

def main():
    print("=" * 60)
    print("Traffic Camera Data Aggregator")
    print("=" * 60)
    print(f"Processing {len(SOURCES)} verified sources...")
    print()

    results = []
    failed = []
    total_cameras = 0

    for i, source in enumerate(SOURCES, 1):
        print(f"[{i}/{len(SOURCES)}]", end=" ")
        result = process_source(source)
        if result:
            results.append(result)
            total_cameras += result["camera_count"]
        else:
            failed.append(source["name"])
        time.sleep(DELAY_BETWEEN_REQUESTS)

    # Create output
    output = {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_cameras": total_cameras,
            "sources_processed": len(results),
            "sources_failed": len(failed),
            "failed_sources": failed,
            "sources_requiring_auth": [s["name"] for s in REQUIRES_AUTH],
            "notes": "Some state DOTs require API keys or registration. See REQUIRES_AUTH in script for details."
        },
        "sources": results
    }

    # Write output
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output, f, indent=2)

    print()
    print("=" * 60)
    print(f"COMPLETE: {total_cameras:,} cameras from {len(results)} sources")
    print(f"FAILED: {len(failed)} sources")
    print(f"REQUIRES AUTH: {len(REQUIRES_AUTH)} sources (not fetched)")
    print(f"Output: {OUTPUT_FILE}")
    print("=" * 60)

    if failed:
        print("\nFailed sources:")
        for name in failed:
            print(f"  - {name}")

if __name__ == "__main__":
    main()
