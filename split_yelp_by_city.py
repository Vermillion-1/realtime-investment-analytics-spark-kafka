import json
import tarfile
import os
from collections import defaultdict
from pathlib import Path


def extract_tar(tar_path, extract_to="yelp_data"):
    """Extract the tar file to a directory."""
    print(f"Extracting {tar_path}...")
    with tarfile.open(tar_path, "r") as tar:
        tar.extractall(path=extract_to)
    print(f"Extraction complete to {extract_to}/")
    return extract_to


def get_city_businesses(business_file, canadian_only=False, specific_cities=None):
    """
    First pass: Build a mapping of cities to business_ids.
    Returns: dict of {city: set(business_ids)}
    """
    # Canadian province codes
    canadian_provinces = {
        "AB",
        "BC",
        "MB",
        "NB",
        "NL",
        "NS",
        "NT",
        "NU",
        "ON",
        "PE",
        "QC",
        "SK",
        "YT",
    }

    print("Analyzing businesses by city...")
    if canadian_only:
        print("Filtering for Canadian cities only...")

    city_businesses = defaultdict(set)
    total_businesses = 0
    filtered_count = 0

    with open(business_file, "r", encoding="utf-8") as f:
        for line in f:
            business = json.loads(line)
            state = business.get("state", "")

            # Skip if filtering for Canadian only and state is not a Canadian province
            if canadian_only and state not in canadian_provinces:
                filtered_count += 1
                continue

            city = business.get("city", "Unknown")
            business_id = business.get("business_id")
            city_businesses[city].add(business_id)
            total_businesses += 1

            if total_businesses % 10000 == 0:
                print(f"  Processed {total_businesses} businesses...")

    if canadian_only:
        print(f"Filtered out {filtered_count} non-Canadian businesses")

    # Filter for specific cities if provided
    if specific_cities:
        city_businesses = {
            city: biz_ids
            for city, biz_ids in city_businesses.items()
            if city in specific_cities
        }
        print(f"Filtered to {len(specific_cities)} specific cities")

    print(
        f"Found {len(city_businesses)} cities with {total_businesses} total businesses"
    )

    # Print city statistics
    print("\nTop 10 cities by business count:")
    sorted_cities = sorted(
        city_businesses.items(), key=lambda x: len(x[1]), reverse=True
    )
    for city, biz_ids in sorted_cities[:10]:
        print(f"  {city}: {len(biz_ids)} businesses")

    return city_businesses


def split_file_by_city(input_file, output_dir, city_businesses, id_field="business_id"):
    """
    Split a JSON file by city based on business_id mapping.
    """
    filename = os.path.basename(input_file)
    print(f"\nProcessing {filename}...")

    # Create file handles for each city
    city_files = {}
    for city in city_businesses.keys():
        safe_city = city.replace("/", "_").replace("\\", "_")
        city_dir = os.path.join(output_dir, safe_city)
        os.makedirs(city_dir, exist_ok=True)
        city_files[city] = open(os.path.join(city_dir, filename), "w", encoding="utf-8")

    # Process the file line by line
    count = 0
    matched = 0
    with open(input_file, "r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            business_id = obj.get(id_field)

            # Find which city this business belongs to
            for city, biz_ids in city_businesses.items():
                if business_id in biz_ids:
                    city_files[city].write(line)
                    matched += 1
                    break

            count += 1
            if count % 50000 == 0:
                print(f"  Processed {count} records ({matched} matched)...")

    # Close all files
    for f in city_files.values():
        f.close()

    print(f"  Completed: {count} total records, {matched} matched to cities")


def split_file_by_city_renamed(
    input_file, output_dir, city_businesses, id_field, output_filename
):
    """
    Split a JSON file by city based on business_id mapping with custom output name.
    Uses a two-pass approach to avoid "too many open files" error.
    """
    print(f"\nProcessing {os.path.basename(input_file)}...")

    # First pass: collect all records by city in memory
    city_records = defaultdict(list)
    count = 0
    matched = 0

    with open(input_file, "r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            business_id = obj.get(id_field)

            # Find which city this business belongs to
            for city, biz_ids in city_businesses.items():
                if business_id in biz_ids:
                    city_records[city].append(line)
                    matched += 1
                    break

            count += 1
            if count % 50000 == 0:
                print(f"  Processed {count} records ({matched} matched)...")

    # Second pass: write each city's records to file
    print(f"  Writing to {len(city_records)} city files...")
    for city, records in city_records.items():
        safe_city = city.replace("/", "_").replace("\\", "_")
        city_dir = os.path.join(output_dir, safe_city)
        os.makedirs(city_dir, exist_ok=True)

        with open(os.path.join(city_dir, output_filename), "w", encoding="utf-8") as f:
            f.writelines(records)

    print(f"  Completed: {count} total records, {matched} matched to cities")


def split_user_file(user_file, output_dir, city_businesses, review_file):
    """
    Split user.json based on users who have reviewed businesses in each city.
    This requires first identifying which users reviewed businesses in each city.
    """
    print("\nIdentifying users per city from reviews...")
    city_users = defaultdict(set)

    with open(review_file, "r", encoding="utf-8") as f:
        count = 0
        for line in f:
            review = json.loads(line)
            business_id = review.get("business_id")
            user_id = review.get("user_id")

            for city, biz_ids in city_businesses.items():
                if business_id in biz_ids:
                    city_users[city].add(user_id)
                    break

            count += 1
            if count % 100000 == 0:
                print(f"  Processed {count} reviews...")

    print(f"Identified users for {len(city_users)} cities")

    # Now split the user file - collect records in memory first
    print("\nSplitting user.json by city...")
    city_records = defaultdict(list)

    count = 0
    matched = 0
    with open(user_file, "r", encoding="utf-8") as f:
        for line in f:
            user = json.loads(line)
            user_id = user.get("user_id")

            # Write user to all cities they've reviewed in
            user_matched = False
            for city, user_ids in city_users.items():
                if user_id in user_ids:
                    city_records[city].append(line)
                    user_matched = True

            if user_matched:
                matched += 1

            count += 1
            if count % 10000 == 0:
                print(f"  Processed {count} users ({matched} matched)...")

    # Write each city's user records
    print(f"  Writing to {len(city_records)} city files...")
    for city, records in city_records.items():
        safe_city = city.replace("/", "_").replace("\\", "_")
        city_dir = os.path.join(output_dir, safe_city)
        os.makedirs(city_dir, exist_ok=True)

        with open(os.path.join(city_dir, "user.json"), "w", encoding="utf-8") as f:
            f.writelines(records)

    print(f"  Completed: {count} total users, {matched} matched to cities")


def main():
    # Configuration
    tar_path = (
        "/home/aarish/Documents/University/CMPT732/Project/Yelp JSON/yelp_dataset"
    )
    extract_dir = "/home/aarish/Documents/University/CMPT732/Project/yelp_data"
    output_dir = "/home/aarish/Documents/University/CMPT732/Project/yelp_by_city"

    # ============= FILTER OPTIONS =============
    # Set to True to only process Canadian cities, False for all cities
    CANADIAN_ONLY = True

    # Optional: Specify specific cities to extract (leave empty list for all)
    # Example: SPECIFIC_CITIES = ['Toronto', 'Montreal', 'Vancouver']
    SPECIFIC_CITIES = []
    # =========================================

    # Step 1: Extract tar file if needed
    if not os.path.exists(extract_dir):
        extract_tar(tar_path, extract_dir)
    else:
        print(f"Using existing extracted data in {extract_dir}/")

    # Find the actual data directory (sometimes there's a subdirectory)
    data_files = list(Path(extract_dir).rglob("*business.json"))
    if not data_files:
        print("ERROR: Could not find business.json in extracted data")
        return

    data_dir = str(data_files[0].parent)
    business_file = str(data_files[0])
    print(f"Using data directory: {data_dir}")
    print(f"Business file: {business_file}")

    # Step 2: Analyze businesses by city
    city_businesses = get_city_businesses(
        business_file,
        canadian_only=CANADIAN_ONLY,
        specific_cities=SPECIFIC_CITIES if SPECIFIC_CITIES else None,
    )

    if not city_businesses:
        print("\n⚠️  No cities found to process!")
        return

    # Step 3: Split business.json by city
    output_name = os.path.basename(business_file).replace("yelp_academic_dataset_", "")
    split_file_by_city_renamed(
        business_file, output_dir, city_businesses, "business_id", output_name
    )

    # Step 4: Split other files by city
    files_to_split = [
        ("*review.json", "business_id"),
        ("*tip.json", "business_id"),
        ("*checkin.json", "business_id"),
        ("*photo.json", "business_id"),
    ]

    for filename_pattern, id_field in files_to_split:
        matching_files = list(Path(data_dir).glob(filename_pattern))
        if matching_files:
            filepath = str(matching_files[0])
            output_name = os.path.basename(filepath).replace(
                "yelp_academic_dataset_", ""
            )
            print(f"\nProcessing {os.path.basename(filepath)} -> {output_name}")
            split_file_by_city_renamed(
                filepath, output_dir, city_businesses, id_field, output_name
            )
        else:
            print(f"Warning: {filename_pattern} not found, skipping...")

    # Step 5: Split user.json (more complex - needs review data)
    review_files = list(Path(data_dir).glob("*review.json"))
    user_files = list(Path(data_dir).glob("*user.json"))
    if review_files and user_files:
        review_file = str(review_files[0])
        user_file = str(user_files[0])
        split_user_file(user_file, output_dir, city_businesses, review_file)

    print(f"\n✅ Complete! City-separated data saved to {output_dir}/")
    print(f"Each city has its own directory with all relevant JSON files.")


if __name__ == "__main__":
    main()
