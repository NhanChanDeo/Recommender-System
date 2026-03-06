import pandas as pd
import argparse
import os

def convert_csv_to_parquet(input_file, output_file=None, sep=',', has_header=True, columns=None):
    """
    Converts a CSV or TSV file to Parquet format.
    """
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Source file not found: {input_file}")
    
    if output_file is None:
        # Default: Replace extension with .parquet
        base_name, _ = os.path.splitext(input_file)
        output_file = f"{base_name}.parquet"
        
    print(f"Reading:  {input_file}")
    print(f"Settings: separator='{sep}', has_header={has_header}")
    
    # Configure headers
    header = 'infer' if has_header else None
    
    # Load the data
    try:
        if columns and not has_header:
            # User provided explicit column names for a file without headers
            df = pd.read_csv(input_file, sep=sep, header=header, names=columns)
        else:
            df = pd.read_csv(input_file, sep=sep, header=header)
            
        print(f"Loaded {len(df):,} rows and {len(df.columns)} columns.")
        
    except Exception as e:
        print(f"❌ Error reading the input file: {e}")
        return

    print("Converting to Parquet using Snappy compression...")
    
    try:
        # Save to parquet
        df.to_parquet(output_file, engine='pyarrow', compression='snappy', index=False)
        
        # Calculate size savings
        original_size = os.path.getsize(input_file) / (1024)
        new_size = os.path.getsize(output_file) / (1024)
        
        print(f"✅ Success! File saved to: {output_file}")
        print(f"📊 Storage Size: {original_size:.4f} KB  ->  {new_size:.4f} KB ({(1 - new_size/original_size)*100:.1f}% reduction)")
        
    except Exception as e:
        print(f"❌ Error saving Parquet file: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert any CSV/TSV to optimized Parquet format.")
    
    parser.add_argument("input_file", help="Path to the input CSV or TSV file.")
    parser.add_argument("--output", "-o", default=None, help="Optional: Path to the output Parquet file.")
    parser.add_argument("--sep", "-s", default=",", help="Delimiter used in the input ('\\t' for TSV). Default is ','.")
    parser.add_argument("--no-header", action="store_true", help="Flag to indicate if the input file has NO header row.")
    parser.add_argument("--cols", "-c", nargs="+", help="List of column names (only useful if --no-header is set).")
    
    args = parser.parse_args()
    
    # Handle the raw string \t that terminal sometimes passes
    separator = '\t' if args.sep == '\\t' else args.sep
    
    convert_csv_to_parquet(
        input_file=args.input_file,
        output_file=args.output,
        sep=separator,
        has_header=not args.no_header,
        columns=args.cols
    )
