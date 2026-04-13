# query_engine.py
import math
import time

def parse_matric_number(matric_number):
    """
    Parse matriculation number to extract query parameters.
    Format example: U2222087K
    
    Returns:
        dict: Contains target_year, start_month, target_towns, unique_digits
    """
    # Extract all digits from matric number
    digits = []
    for char in matric_number:
        if char.isdigit():
            digits.append(int(char))
    
    if len(digits) < 2:
        raise ValueError("Matric number must contain at least 2 digits")
    
    # Target year: find year between 2015-2024 whose last digit matches
    # Example: last digit 7 -> 2017
    last_digit = digits[-1]
    target_year = last_digit + 2010
    if target_year < 2015:
        target_year += 10
    
    # Start month: second last digit, with 0 representing October
    # Example: second last digit 8 -> August, 0 -> October
    second_last = digits[-2]
    start_month = 10 if second_last == 0 else second_last
    
    # Get unique digits and sort them for consistent order
    unique_digits = sorted(set(digits))
    
    return {
        "target_year": target_year,
        "start_month": start_month,
        "target_towns": unique_digits,
    }


def run_queries(columns, matric_number,x_range = [1, 8], y_range = [80, 150] ):
    """
    Execute all queries for x = 1..8 and y = 80..150.
    
    Args:
        columns: Dictionary containing columnar data storage
        matric_number: Matriculation number for query conditions
    
    Returns:
        list: List of dictionaries containing valid (x,y) pair results
    """
    # Start total timer
    total_start_time = time.time()

    # Parse matric number to get query parameters
    params = parse_matric_number(matric_number)
    target_year = params["target_year"]
    start_month = params["start_month"]
    target_towns = params["target_towns"]
    
    print(f"Matric Number: {matric_number}")
    print(f"Target Year: {target_year}")
    print(f"Start Month: {start_month}")
    print(f"Target Towns: {target_towns}")
    print()
    
    results = []

    x_min = x_range[0]
    x_max = x_range[1]
    y_min = y_range[0]
    y_max = y_range[1]


    # Iterate over all (x, y) pairs
    for x in range(x_min, x_max+1):  # x: 1 to 8 inclusive by default
        for y in range(y_min, y_max+1):  # y: 80 to 150 inclusive by default
            # Calculate end month based on x months from start
            end_month = start_month + x - 1
            end_year = target_year
            
            # Handle year rollover if end_month exceeds 12
            while end_month > 12:
                end_month -= 12
                end_year += 1
            
            # Find all records matching the conditions
            matching_indices = find_matching_records(
                columns, 
                target_year, start_month, 
                end_year, end_month,
                target_towns, 
                y
            )
            
            # Skip if no records match the conditions
            if not matching_indices:
                continue
            
            # Calculate price per square meter for each matching record
            best_record = None
            best_price_per_sqm = float('inf')
            
            for idx in matching_indices:
                price_per_sqm = columns["price"][idx] / columns["area"][idx]
                
                # Only consider records with price <= 4725
                if price_per_sqm <= 4725 and price_per_sqm < best_price_per_sqm:
                    best_price_per_sqm = price_per_sqm
                    best_record = {
                        "index": idx,
                        "price_per_sqm": price_per_sqm
                    }
            
            # If a valid record (price <= 4725) is found, add to results
            if best_record:
                idx = best_record["index"]
                results.append({
                    "x": x,
                    "y": y,
                    "year": columns["year"][idx],
                    "month": columns["month"][idx],
                    "town": columns["town"][idx],
                    "block": columns["block"][idx],
                    "floor_area": columns["area"][idx],
                    "flat_model": columns["model"][idx],
                    "lease_commence_date": columns["lease"][idx],
                    "price_per_sqm": round(best_price_per_sqm)  # Round to nearest integer
                })
    
    # Sort results by x ascending, then y ascending
    results.sort(key=lambda r: (r["x"], r["y"]))
    
    print(f"Found {len(results)} valid (x, y) pairs")
    
    total_time = time.time() - total_start_time
    print(f"TOTAL TIME: {total_time:.2f} seconds")
    print()
    
    return results


def find_matching_records(columns, start_year, start_month, end_year, end_month, target_towns, min_area):
    """
    Find all record indices that match the given conditions.
    
    Args:
        columns: Columnar data storage
        start_year: Start year for time range
        start_month: Start month for time range
        end_year: End year for time range
        end_month: End month for time range
        target_towns: List of town names to include
        min_area: Minimum floor area requirement
    
    Returns:
        list: Indices of matching records
    """
    indices = []
    
    for i in range(len(columns["year"])):
        year = columns["year"][i]
        month = columns["month"][i]
        
        # Check time range
        if year < start_year or year > end_year:
            continue
        if year == start_year and month < start_month:
            continue
        if year == end_year and month > end_month:
            continue
        
        # Check if town is in target list
        if columns["town"][i] not in target_towns:
            continue
        
        # Check area requirement
        if columns["area"][i] < min_area:
            continue
        
        indices.append(i)
    
    return indices
