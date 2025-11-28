import json
import tarfile
import os
from collections import defaultdict
from pathlib import Path


def extract_tar(tar_path, extract_to="yelp_data"):
    print(f"Extracting {tar_path}...")
    with tarfile.open(tar_path, "r") as tar:
        tar.extractall(path=extract_to)
    print(f"Extraction complete to {extract_to}/")
    return extract_to


def get_city_businesses(business_file, min_businesses=100):
    us_states = {
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
        "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
        "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
        "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
        "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
    }

    print("Analyzing businesses by city...")
    city_businesses = defaultdict(set)
    
    with open(business_file, "r", encoding="utf-8") as f:
        for line in f:
            business = json.loads(line)
            if business.get("state") not in us_states:
                continue

            city = business.get("city", "Unknown")
            city_businesses[city].add(business.get("business_id"))

    # Filter cities
    city_businesses = {
        c: b for c, b in city_businesses.items() 
        if len(b) >= min_businesses
    }
    
    print(f"Found {len(city_businesses)} US cities with >={min_businesses} businesses")
    return city_businesses


def split_file_by_city(input_file, output_dir, city_businesses, id_field, output_filename):
    print(f"Processing {os.path.basename(input_file)}...")
    
    # Collect records
    city_records = defaultdict(list)
    matched = 0
    
    with open(input_file, "r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            bid = obj.get(id_field)
            
            for city, biz_ids in city_businesses.items():
                if bid in biz_ids:
                    city_records[city].append(line)
                    matched += 1
                    break

    # Write files
    print(f"  Writing to {len(city_records)} city files...")
    for city, records in city_records.items():
        city_dir = os.path.join(output_dir, city.replace("/", "_"))
        os.makedirs(city_dir, exist_ok=True)
        
        with open(os.path.join(city_dir, output_filename), "w", encoding="utf-8") as f:
            f.writelines(records)
            
    print(f"  Matched {matched} records")


def split_user_file(user_file, output_dir, city_businesses, review_file):
    print("Mapping users to cities...")
    city_users = defaultdict(set)

    # Map users to cities via reviews
    with open(review_file, "r", encoding="utf-8") as f:
        for line in f:
            review = json.loads(line)
            bid = review.get("business_id")
            uid = review.get("user_id")
            
            for city, biz_ids in city_businesses.items():
                if bid in biz_ids:
                    city_users[city].add(uid)
                    break

    print("Splitting user.json...")
    city_records = defaultdict(list)
    
    with open(user_file, "r", encoding="utf-8") as f:
        for line in f:
            user = json.loads(line)
            uid = user.get("user_id")
            
            for city, user_ids in city_users.items():
                if uid in user_ids:
                    city_records[city].append(line)

    # Write files
    for city, records in city_records.items():
        city_dir = os.path.join(output_dir, city.replace("/", "_"))
        os.makedirs(city_dir, exist_ok=True)
        
        with open(os.path.join(city_dir, "user.json"), "w", encoding="utf-8") as f:
            f.writelines(records)
            
    print(f"  Processed users for {len(city_records)} cities")


def main():
    base_dir = "/Users/aarish/Documents/MSCompSci/CMPT732/project/test"
    tar_path = os.path.join(base_dir, "yelp_dataset")
    extract_dir = os.path.join(base_dir, "yelp_data")
    output_dir = os.path.join(base_dir, "yelp_by_city")

    # Setup data directory
    if os.path.isdir(tar_path):
        data_dir = tar_path
    elif not os.path.exists(extract_dir):
        extract_tar(tar_path, extract_dir)
        data_dir = extract_dir
    else:
        data_dir = extract_dir

    # Find business.json
    data_files = list(Path(data_dir).rglob("*business.json"))
    if not data_files:
        print("Error: business.json not found")
        return
        
    business_file = str(data_files[0])
    data_dir = str(data_files[0].parent)

    # 1. Get cities
    city_businesses = get_city_businesses(business_file)
    if not city_businesses:
        print("No matching cities found")
        return

    # 2. Split business.json
    split_file_by_city(
        business_file, 
        output_dir, 
        city_businesses, 
        "business_id", 
        "business.json"
    )

    # 3. Split other files
    files = [
        ("*review.json", "business_id", "review.json"),
        ("*tip.json", "business_id", "tip.json"),
        ("*checkin.json", "business_id", "checkin.json"),
        ("*photo.json", "business_id", "photo.json"),
    ]

    for pattern, id_field, out_name in files:
        found = list(Path(data_dir).glob(pattern))
        if found:
            split_file_by_city(
                str(found[0]), 
                output_dir, 
                city_businesses, 
                id_field, 
                out_name
            )

    # 4. Split user.json
    reviews = list(Path(data_dir).glob("*review.json"))
    users = list(Path(data_dir).glob("*user.json"))
    
    if reviews and users:
        split_user_file(str(users[0]), output_dir, city_businesses, str(reviews[0]))

    print(f"\nComplete. Data saved to {output_dir}/")


if __name__ == "__main__":
    main()
