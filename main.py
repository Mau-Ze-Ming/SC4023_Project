from data_storage import load_data
from query_engine import run_queries
from output_writer import write_output

def main():
    file_path = "ResalePricesSingapore.csv"

    columns = load_data(file_path)
    results = run_queries(columns)

    write_output(results, "ScanResult_UxxxxxxxG.csv") #put your matric number in 

if __name__ == "__main__":
    main()
