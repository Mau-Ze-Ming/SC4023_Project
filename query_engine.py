# query_engine.py
import math
import time

def parse_matric_number(matric_number):
    """
    Parse matriculation number to extract query parameters.
    Format example: U2222087K
    
    Returns:
        dict: Contains target_year, start_month, target_towns
    """
    # Extract all digits from matric number
    digits = []
    for char in matric_number:
        if char.isdigit():
            digits.append(int(char))
    
    if len(digits) < 2:
        raise ValueError("Matric number must contain at least 2 digits")
    
    # Target year: find year between 2015-2024 whose last digit matches
    last_digit = digits[-1]
    target_year = last_digit + 2010
    if target_year < 2015:
        target_year += 10
    
    # Start month: second last digit, with 0 representing October
    second_last = digits[-2]
    start_month = 10 if second_last == 0 else second_last
    
    # Get unique digits for towns (return as set for faster lookup)
    unique_digits = set(digits)
    
    return {
        "target_year": target_year,
        "start_month": start_month,
        "target_towns": sorted(unique_digits)
    }

def prefilter_dataset(columns, target_year, start_month, target_towns, x_max=8, y_min=80, price_max=4725):
    """
    Pre-filter the entire dataset to get candidate indices for all queries.
    This reduces the search space for individual queries.
    
    Args:
        columns: Columnar data storage
        target_year: Target year from matric number
        start_month: Start month from matric number
        target_towns: Set of target town codes
        x_max: Maximum x value (default 8)
        y_min: Minimum y value (default 80)
        price_max: Maximum price per square meter (default 4725)
    
    Returns:
        list: Candidate indices that could possibly match any query
    """
    # Calculate maximum time range (when x = x_max)
    max_end_month = start_month + x_max - 1
    max_end_year = target_year
    
    # Handle year rollover
    while max_end_month > 12:
        max_end_month -= 12
        max_end_year += 1
    
    print(f"Pre-filtering: Time range {target_year}-{start_month:02d} to {max_end_year}-{max_end_month:02d}")
    print(f"Town Names: {get_town_name(target_towns)}")
    print(f"Min area: {y_min}")
    print(f"Max price per sqm: {price_max}")
    
    # Scan all records to find candidates
    candidate_indices = []
    scan_range = range(len(columns["year"]))
    
    for i in scan_range:
        year = columns["year"][i]
        month = columns["month"][i]
        
        # Check time range
        if year < target_year or year > max_end_year:
            continue
        if year == target_year and month < start_month:
            continue
        if year == max_end_year and month > max_end_month:
            continue
        
        # Check town
        if columns["town"][i] not in target_towns:
            continue
        
        # Check minimum area (y_min = 80)
        if columns["area"][i] < y_min:
            continue
        
        # Check price per square meter (must be <= price_max to ever be valid)
        price_per_sqm = columns["price"][i] / columns["area"][i]
        if price_per_sqm > price_max:
            continue
        
        candidate_indices.append(i)
    
    print(f"Pre-filtered {len(candidate_indices)} candidate records (from {len(columns['year'])})")
    
    return candidate_indices

def get_month_key(year, month):
    """
    Generate a unique key for a specific month.
    """
    return (year, month)

def get_next_month(year, month):
    """
    Get the next month (handles year rollover).
    """
    month += 1
    if month > 12:
        month = 1
        year += 1
    return year, month

def get_town_name(code):
    """
    Convert town code to town name.
    If input is a set or list, returns list of names.
    If input is a single integer, returns single name.
    
    Args:
        code: Integer town code (0-9) or set/list of codes
    
    Returns:
        str or list: Town name(s), or "UNKNOWN" if code not found
    """
    reverse_town_map = {
        0: "BEDOK",
        1: "BUKIT PANJANG",
        2: "CLEMENTI",
        3: "CHOA CHU KANG",
        4: "HOUGANG",
        5: "JURONG WEST",
        6: "PASIR RIS",
        7: "TAMPINES",
        8: "WOODLANDS",
        9: "YISHUN"
    }
    
    # If input is a set or list, return list of names
    if isinstance(code, (set, list)):
        return [reverse_town_map.get(c, "UNKNOWN") for c in sorted(code)]
    
    # If input is a single integer, return single name
    return reverse_town_map.get(code, "UNKNOWN")

def precompute_monthly_best(columns, target_towns, months_list, candidate_indices, y_min, y_max, price_max):
    """
    Pre-compute best records for all months and all y values in ONE PASS per month.
    Uses descending y order to process each month's records only once.
    
    Returns:
        dict: monthly_best[y][month_key] = {"idx": idx, "price": price, "area": area}
    """
    monthly_best = {y: {} for y in range(y_min, y_max + 1)}
    
    # For each month
    for year, month in months_list:
        # Step 1: Collect all records in this month from candidate_indices
        month_records = []
        for idx in candidate_indices:
            if columns["year"][idx] == year and columns["month"][idx] == month:
                if columns["town"][idx] in target_towns:
                    price_per_sqm = columns["price"][idx] / columns["area"][idx]
                    if price_per_sqm <= price_max:
                        month_records.append({
                            "idx": idx,
                            "area": columns["area"][idx],
                            "price": price_per_sqm
                        })
        
        # Step 2: Sort by area descending
        month_records.sort(key=lambda r: r["area"], reverse=True)
        
        # Step 3: One-pass scan to compute best for all y
        best_price = float('inf')
        best_idx = None
        record_ptr = 0
        num_records = len(month_records)
        
        for y in range(y_max, y_min - 1, -1):
            # Add all records with area >= y
            while record_ptr < num_records and month_records[record_ptr]["area"] >= y:
                if month_records[record_ptr]["price"] < best_price:
                    best_price = month_records[record_ptr]["price"]
                    best_idx = month_records[record_ptr]["idx"]
                record_ptr += 1
            
            if best_idx is not None:
                monthly_best[y][(year, month)] = {
                    "idx": best_idx,
                    "price": best_price,
                    "area": month_records[record_ptr - 1]["area"] if record_ptr > 0 else 0
                }
    
    return monthly_best


def run_queries(columns, matric_number, x_range=(1, 8), y_range=(80, 150), max_price=4725):
    """
    Execute all queries using optimized pre-computation.
    """
    # Start total timer
    total_start_time = time.time()
    
    # Parse matric number
    params = parse_matric_number(matric_number)
    target_year = params["target_year"]
    start_month = params["start_month"]
    target_towns = params["target_towns"]
    
    # Extract range values
    x_min, x_max = x_range
    y_min, y_max = y_range
    
    print(f"Matric Number: {matric_number}")
    print(f"Target Year: {target_year}")
    print(f"Start Month: {start_month}")
    print(f"Target Town Codes: {target_towns}")
    print(f"Target Town Names: {get_town_name(target_towns)}")
    print(f"X Range: {x_min} to {x_max}")
    print(f"Y Range: {y_min} to {y_max}")
    print(f"Max Price: {max_price}")
    print()
    
    # Step 1: Pre-filter the dataset
    candidate_indices = prefilter_dataset(
        columns, target_year, start_month, target_towns, 
        x_max=x_max, y_min=y_min, price_max=max_price
    )
    print()

    if not candidate_indices:
        return []
    
    # Step 2: Generate list of months
    months_list = []
    current_year, current_month = target_year, start_month
    for x in range(1, x_max + 1):
        months_list.append((current_year, current_month))
        current_year, current_month = get_next_month(current_year, current_month)
    
    # Step 3: Pre-compute monthly best records
    print("Pre-computing monthly best records...")
    precompute_start = time.time()
    
    monthly_best = precompute_monthly_best(
        columns, target_towns, months_list, candidate_indices, y_min, y_max, max_price
    )
    
    precompute_time = time.time() - precompute_start
    print(f"Monthly pre-computation completed in {precompute_time:.2f} seconds")
    print()
    print("Generating final query results...")
    
    # Step 4: Incrementally compute results for each x and y
    results = []
    
    for y in range(y_min, y_max + 1):
        cumulative_best_idx = None
        cumulative_best_price = float('inf')
        
        for x in range(x_min, x_max + 1):
            year, month = months_list[x-1]
            month_key = (year, month)
            
            # Get this month's best from pre-computed cache
            if month_key in monthly_best[y]:
                month_best = monthly_best[y][month_key]
                month_price = month_best["price"]
                month_idx = month_best["idx"]
                
                if month_price < cumulative_best_price:
                    cumulative_best_price = month_price
                    cumulative_best_idx = month_idx
            
            if cumulative_best_idx is not None and cumulative_best_price <= max_price:
                results.append({
                    "x": x,
                    "y": y,
                    "year": columns["year"][cumulative_best_idx],
                    "month": columns["month"][cumulative_best_idx],
                    "town": columns["town"][cumulative_best_idx],
                    "block": columns["block"][cumulative_best_idx],
                    "floor_area": columns["area"][cumulative_best_idx],
                    "flat_model": columns["model"][cumulative_best_idx],
                    "lease_commence_date": columns["lease"][cumulative_best_idx],
                    "price_per_sqm": round(cumulative_best_price)
                })
    
    # Sort results
    results.sort(key=lambda r: (r["x"], r["y"]))
    
    total_time = time.time() - total_start_time
    print(f"Found {len(results)} valid (x, y) pairs")
    print(f"TOTAL TIME: {total_time:.2f} seconds")
    
    return results
