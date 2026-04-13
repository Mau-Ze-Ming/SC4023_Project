# output_writer.py
import csv

def write_output(results, output_filename):
    """
    Write query results to CSV file.
    
    Args:
        results: List of dictionaries containing query results
        output_filename: Name of the output CSV file
    """
    with open(output_filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Write header row
        writer.writerow([
            '(x,y)',
            'Year',
            'Month',
            'Town',
            'Block',
            'Floor_Area',
            'Flat_Model',
            'Lease_Commence_Date',
            'Price_Per_Square_Meter'
        ])
        
        # Write each result
        for record in results:
            writer.writerow([
                f"({record['x']}, {record['y']})",
                record['year'],
                f"{record['month']:02d}",  # Format month with leading zero (e.g., 02)
                record['town'],
                record['block'],
                f"{record['floor_area']:.0f}",  # Floor area as integer
                record['flat_model'],
                record['lease_commence_date'],
                record['price_per_sqm']
            ])
    
    print(f"Output written to {output_filename}")