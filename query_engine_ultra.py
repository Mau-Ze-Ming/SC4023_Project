# query_engine.py
import math
import time

def parse_matric_number(matric_number):
    """
    Parse matriculation number to extract query parameters.
    Format example: U2222087K
    
    Returns:
        dict: Contains target_year, start_month, target_town_digits, unique_digits
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


def find_records_in_single_month(columns, target_towns, min_area, year, month, indices=None, price_max=4725):
    """
    Find all records that match conditions in a single month.
    
    Args:
        columns: Columnar data storage
        target_towns: Set of town codes (integers)
        min_area: Minimum floor area requirement (y)
        year: Target year
        month: Target month
        indices: Optional pre-filtered indices (already filtered by time, town, area, price)
        price_max: Maximum price per square meter (default 4725)
    
    Returns:
        tuple: (best_idx, best_price_per_sqm) or (None, None) if no record found
    """
    # Determine which indices to scan
    if indices is None:
        scan_range = range(len(columns["year"]))
    else:
        scan_range = indices
    
    best_idx = None
    best_price = float('inf')
    
    for i in scan_range:
        # Check if record is in the target month
        if columns["year"][i] != year or columns["month"][i] != month:
            continue
        
        # Check area
        if columns["area"][i] < min_area:
            continue
        
        # Calculate price per square meter
        price_per_sqm = columns["price"][i] / columns["area"][i]
        
        # Only consider records with price <= price_max
        if price_per_sqm <= price_max and price_per_sqm < best_price:
            best_price = price_per_sqm
            best_idx = i
    
    return best_idx, best_price


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


def run_queries(columns, matric_number, x_range=(1, 8), y_range=(80, 150), max_price=4725):
    """
    Execute all queries for x in x_range and y in y_range.
    Uses incremental optimization:
    - For fixed y, results for x can be derived from x-1 + new month data
    
    Args:
        columns: Dictionary containing columnar data storage
        matric_number: Matriculation number
        x_range: Tuple of (min_x, max_x), default (1, 8)
        y_range: Tuple of (min_y, max_y), default (80, 150)
        max_price: Maximum price per square meter threshold, default 4725
    
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
    
    # Step 1: Pre-filter the entire dataset
    candidate_indices = prefilter_dataset(
        columns, target_year, start_month, target_towns, 
        x_max=x_max, y_min=y_min, price_max=max_price
    )
    print()

    if not candidate_indices:
        return []
    
    # Step 2: Pre-compute best records for each individual month
    print("Pre-computing monthly best records...")
    
    # Generate list of months in the range (x=1 to x_max)
    months_list = []
    current_year, current_month = target_year, start_month
    for x in range(1, x_max + 1):
        months_list.append((current_year, current_month))
        current_year, current_month = get_next_month(current_year, current_month)
    
    # For each y, store the best record for each month
    # Structure: monthly_best[y][month_key] = (best_idx, best_price)
    monthly_best = {}
    
    # Process each y in the specified range (from high to low for optimization)
    for y in range(y_max, y_min - 1, -1):
        monthly_best[y] = {}
        for year, month in months_list:
            best_idx, best_price = find_records_in_single_month(
                columns, target_towns, y, year, month, candidate_indices, max_price
            )
            if best_idx is not None:
                monthly_best[y][(year, month)] = {
                    "idx": best_idx,
                    "price": best_price,
                    "area": columns["area"][best_idx]
                }
    
    print("Monthly pre-computation complete!")
    print()
    print("Generating final query results...")
    
    # Step 3: Incrementally compute results for each x and y
    # For fixed y, result for x = min(result for x-1, best in month x)
    results = []
    
    # For each y in the specified range
    for y in range(y_min, y_max + 1):
        # Track cumulative best as x increases
        cumulative_best_idx = None
        cumulative_best_price = float('inf')
        cumulative_best_area = 0
        
        # For each x in the specified range (increasing)
        for x in range(x_min, x_max + 1):
            # Get the month for this x (the new month to add)
            # Note: months_list is 0-indexed, so x-1 gives the correct month
            year, month = months_list[x-1]
            month_key = (year, month)
            
            # Add this month's best to cumulative results
            if month_key in monthly_best[y]:
                month_best = monthly_best[y][month_key]
                month_price = month_best["price"]
                month_idx = month_best["idx"]
                
                # Update cumulative best if this month has a better price
                if month_price < cumulative_best_price:
                    cumulative_best_price = month_price
                    cumulative_best_idx = month_idx
                    cumulative_best_area = month_best["area"]
            
            # The result for (x, y) is the cumulative best so far
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
    
    # Sort results by x ascending, then y ascending
    results.sort(key=lambda r: (r["x"], r["y"]))
    
    print(f"Found {len(results)} valid (x, y) pairs")

    total_time = time.time() - total_start_time
    print(f"TOTAL TIME: {total_time:.2f} seconds")
    print()
    
    return results
