from data_storage import load_data
from query_engine import run_queries
from output_writer import write_output

def main():
    file_path = "ResalePricesSingapore.csv"
    MATRIC_NUMBER = "U2222087K"

    columns = load_data(file_path)
    results = run_queries(columns, MATRIC_NUMBER)
    write_output(results, f"ScanResult_{MATRIC_NUMBER}.csv")

if __name__ == "__main__":
    main()
