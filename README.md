# SC4023_Project
# HDB Resale Flat Analysis (Column-Oriented Storage)

## 📌 Project Overview

This project analyzes HDB resale flat transactions in Singapore using a **column-oriented data storage approach**.
The system processes historical transaction data (2015–2025) to compute the **minimum price per square meter** under specific query conditions derived from a matriculation number.

The implementation follows principles of **big data management**, including efficient data storage, filtering, and query processing.

---

## ⚙️ Requirements

* Python 3.x
* No external libraries required (only built-in modules)

---

## ▶️ How to Run

1. Ensure all files are in the same directory:

```
main.py
data_storage.py
query_engine.py
output_writer.py
ResalePricesSingapore.csv
```

2. Run the program:

```
python main.py
```

3. The output will be generated as:

```
ScanResult_UxxxxxxG.csv
```

---

## 📂 Project Structure

```
.
├── main.py                  # Entry point of the program
├── data_storage.py          # Column-oriented data loading (Person 1)
├── query_engine.py          # Query processing logic (Person 2)
├── output_writer.py         # Output formatting and CSV writing (Person 3)
├── ResalePricesSingapore.csv
└── README.md
```

---

## 🧠 Key Features

### 1. Column-Oriented Storage

Data is stored in separate arrays (columns) instead of row-based records, improving efficiency for analytical queries.

### 2. Data Encoding

* Town names are encoded into integers
* Month values are parsed into numeric format

### 3. Efficient Data Processing

* Only required columns are stored
* Data is processed using streaming (row-by-row reading)
* Designed to handle large datasets

### 4. Query Processing

The system evaluates all combinations of:

* `x` = 1 to 8 months
* `y` = 80 to 150 sqm

For each pair `(x, y)`:

* Filters by year, month range, town, and floor area
* Computes price per square meter
* Returns the minimum value if ≤ 4725

---

## 📊 Output Format

Each row in the output file contains:

```
(x,y),Year,Month,Town,Block,Floor_Area,Flat_Model,Lease_Commence_Date,Price_Per_Square_Meter
```

If no valid result exists:

```
(x,y),No result
```

---

## 🧪 Notes

* The dataset uses the format `Jan-15` for dates and is parsed accordingly.
* The program avoids high-level libraries (e.g., pandas) to comply with assignment requirements.
* File paths are relative to ensure portability across systems.

---

## 👥 Team Responsibilities

* **Person 1**: Data Storage (Column Store Implementation)
* **Person 2**: Query Engine (Filtering & Computation)
* **Person 3**: Output Writer (CSV Formatting & Export)

---

## ✅ Usage Notes for Instructor

Simply run:

```
python main.py
```

No additional setup is required.
All paths are preconfigured for immediate execution.

---
