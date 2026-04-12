import csv

TOWN_MAP = {
    "BEDOK": 0,
    "BUKIT PANJANG": 1,
    "CLEMENTI": 2,
    "CHOA CHU KANG": 3,
    "HOUGANG": 4,
    "JURONG WEST": 5,
    "PASIR RIS": 6,
    "TAMPINES": 7,
    "WOODLANDS": 8,
    "YISHUN": 9
}

MONTH_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
    "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
    "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12
}


def parse_month(month_str):
    """
    Convert '15-Jan' → (2015, 1)
    """
    try:
        y_part, m_part = month_str.strip().split('-')
        year = 2000 + int(y_part)
        month = MONTH_MAP[m_part.upper()]
        return year, month
    except:
        return None, None


def encode_town(town_str):
    """
    Encode only known towns, others = -1
    """
    return TOWN_MAP.get(town_str.strip().upper(), -1)


def load_data(file_path):
    # --- Column store ---
    year = []
    month = []
    town = []
    block = []
    area = []
    model = []
    lease = []
    price = []

    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)

        for row in reader:
            y, m = parse_month(row[0])
            if y is None:
                continue

       
            try:
                town_str = row[1]
                blk = row[3]
                a = float(row[6])           
                mdl = row[7]
                lease_year = int(row[8])
                p = float(row[9])
            except:
                continue


            t_id = encode_town(town_str)


            year.append(y)
            month.append(m)
            town.append(t_id)
            block.append(blk)
            area.append(a)
            model.append(mdl)
            lease.append(lease_year)
            price.append(p)

    return {
        "year": year,
        "month": month,
        "town": town,
        "block": block,
        "area": area,
        "model": model,
        "lease": lease,
        "price": price
    }
