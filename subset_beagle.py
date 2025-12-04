#!/usr/bin/env python3
"""
Ultra-Fast Beagle File Subsetting Script

Usage:
    subset_beagle --input <file> --keep <samples> --out <output>
    subset_beagle --input <file> --remove <samples> --out <output>

Author: Holden
"""

import sys
import gzip
import os
import argparse
import subprocess
import tempfile
from typing import Set, List, Tuple

def read_sample_list(sample_list_file: str) -> Set[str]:
    """Read sample IDs from a text file (one per line, no header)"""
    samples = set()
    
    try:
        with open(sample_list_file, 'r') as f:
            for line in f:
                sample_id = line.strip()
                if sample_id:  # Skip empty lines
                    samples.add(sample_id)
    except FileNotFoundError:
        print(f"Error: Sample list file '{sample_list_file}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading sample list file: {e}")
        sys.exit(1)
    
    if not samples:
        print("Error: No samples found in the sample list file.")
        sys.exit(1)
    
    return samples

def read_header(input_file: str) -> str:
    """Read the header line from a Beagle file (handles gzipped files)"""
    try:
        if input_file.endswith('.gz'):
            with gzip.open(input_file, 'rt', encoding='utf-8') as f:
                return f.readline().strip()
        else:
            with open(input_file, 'r', encoding='utf-8') as f:
                return f.readline().strip()
    except FileNotFoundError:
        print(f"Error: Input file '{input_file}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading input file: {e}")
        sys.exit(1)

def find_columns_to_keep(header_line: str, sample_list: Set[str], 
                        remove_mode: bool) -> Tuple[List[int], Set[str], Set[str]]:
    """
    Determine which columns to keep based on sample list
    
    Args:
        header_line: Header line from Beagle file
        sample_list: Set of sample IDs to keep or remove
        remove_mode: If True, remove samples in list; if False, keep only samples in list
    
    Returns:
        Tuple of (column_indices, samples_kept, samples_removed)
    """
    fields = header_line.split('\t')
    
    # Always keep the first 3 columns (marker, allele1, allele2)
    columns_to_keep = [1, 2, 3]  # AWK is 1-indexed!
    
    # Each sample has 3 columns in Beagle format
    all_samples = set()
    samples_kept = set()
    
    for i in range(3, len(fields), 3):
        if i < len(fields):
            sample_id = fields[i]
            all_samples.add(sample_id)
            
            # In remove mode: keep if NOT in list
            # In keep mode: keep if IN list
            should_keep = (sample_id not in sample_list) if remove_mode else (sample_id in sample_list)
            
            if should_keep:
                # Add all 3 columns for this sample (AWK is 1-indexed, so add 1)
                columns_to_keep.extend([i+1, i+2, i+3])
                samples_kept.add(sample_id)
    
    samples_removed = all_samples - samples_kept
    
    # Validation
    if not samples_kept:
        print("Error: No samples would remain after subsetting.")
        sys.exit(1)
    
    return columns_to_keep, samples_kept, samples_removed

def check_pv_available() -> bool:
    """Check if pv (Pipe Viewer) is installed"""
    try:
        subprocess.run(['pv', '--version'], capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False

def generate_awk_command(columns: List[int], input_file: str, output_file: str, 
                        use_progress: bool = True) -> List[str]:
    """
    Generate the AWK command for subsetting
    
    Args:
        columns: List of column indices to keep (1-indexed for AWK)
        input_file: Input Beagle file path
        output_file: Output file path
        use_progress: If True and pv is available, show progress bar
    
    Returns:
        Command as list of strings for subprocess
    """
    # Build the AWK print statement
    # Format: print $1, $2, $3, ... with tab separator
    column_refs = ','.join([f'${col}' for col in columns])
    awk_script = f'{{OFS="\\t"; print {column_refs}}}'
    
    # Determine if files are gzipped
    input_is_gz = input_file.endswith('.gz')
    output_is_gz = output_file.endswith('.gz') or output_file.endswith('.beagle.gz')
    
    # Get file size for progress bar
    file_size = os.path.getsize(input_file)
    
    # Check if pv is available
    has_pv = use_progress and check_pv_available()
    
    # Build pv command if available
    if has_pv:
        pv_cmd = f"pv -p -t -e -r -b -s {file_size}"
    else:
        pv_cmd = "cat"
    
    # Build command pipeline
    if input_is_gz and output_is_gz:
        # zcat input | pv | awk | gzip > output
        cmd = f"zcat {input_file} | {pv_cmd} | awk '{awk_script}' | gzip > {output_file}"
    elif input_is_gz and not output_is_gz:
        # zcat input | pv | awk > output
        cmd = f"zcat {input_file} | {pv_cmd} | awk '{awk_script}' > {output_file}"
    elif not input_is_gz and output_is_gz:
        # pv input | awk | gzip > output
        cmd = f"{pv_cmd} {input_file} | awk '{awk_script}' | gzip > {output_file}"
    else:
        # pv input | awk > output
        cmd = f"{pv_cmd} {input_file} | awk '{awk_script}' > {output_file}"
    
    return ['bash', '-c', cmd], has_pv

def subset_beagle(input_file: str, sample_list_file: str, output_file: str, 
                  remove_mode: bool = False):
    """
    Main function to subset Beagle file using AWK
    
    Args:
        input_file: Path to input Beagle file
        sample_list_file: Path to sample list file
        output_file: Path to output file
        remove_mode: If True, remove samples; if False, keep samples
    """
    print("=" * 70)
    print("BEAGLE FILE SUBSETTING (Python + AWK Hybrid)")
    print("=" * 70)
    
    # Validate input file
    if not os.path.exists(input_file):
        print(f"Error: Input file '{input_file}' not found.")
        sys.exit(1)
    
    # Add .beagle extension if needed
    if not output_file.endswith('.beagle') and not output_file.endswith('.beagle.gz'):
        output_file = output_file + '.beagle'
    
    # Read sample list
    print(f"\n[1/4] Reading sample list from: {sample_list_file}")
    sample_list = read_sample_list(sample_list_file)
    print(f"      Found {len(sample_list)} samples in list")
    
    # Read header
    print(f"\n[2/4] Reading header from: {input_file}")
    header_line = read_header(input_file)
    total_samples = (len(header_line.split('\t')) - 3) // 3
    print(f"      Total samples in file: {total_samples}")
    
    # Find columns to keep
    print(f"\n[3/4] Calculating columns to keep...")
    columns_to_keep, samples_kept, samples_removed = find_columns_to_keep(
        header_line, sample_list, remove_mode
    )
    
    # Report results
    mode_str = "REMOVE" if remove_mode else "KEEP"
    print(f"      Mode: {mode_str}")
    print(f"      Samples kept: {len(samples_kept)}")
    if remove_mode:
        removed_found = sample_list & samples_removed
        removed_not_found = sample_list - samples_removed - samples_kept
        print(f"      Samples removed: {len(removed_found)}")
        if removed_not_found:
            print(f"      Warning: Samples not found in file: {sorted(removed_not_found)}")
    else:
        print(f"      Samples in list kept: {len(samples_kept)}")
        not_found = sample_list - samples_kept
        if not_found:
            print(f"      Warning: Samples not found in file: {sorted(not_found)}")
    
    print(f"      Total columns to extract: {len(columns_to_keep)}")
    
    # Generate and execute AWK command
    print(f"\n[4/4] Processing file with AWK (streaming mode)...")
    print(f"      Input:  {input_file}")
    print(f"      Output: {output_file}")
    
    cmd, has_pv = generate_awk_command(columns_to_keep, input_file, output_file)
    
    if has_pv:
        print(f"\n      Progress (% data processed, time elapsed, rate):")
    else:
        print(f"\n      Note: Install 'pv' for progress bar (conda install pv or apt install pv)")
        print(f"      Processing... (this may take a few moments)")
    
    try:
        # Execute the command
        # Use subprocess.Popen to allow real-time output from pv
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        # Stream output in real-time (for pv progress)
        if has_pv:
            for line in process.stdout:
                print(f"      {line}", end='', flush=True)
        
        # Wait for completion
        return_code = process.wait()
        
        if return_code != 0:
            print(f"\nError: AWK command failed with return code {return_code}")
            sys.exit(1)
        
        # Check if output file was created
        if os.path.exists(output_file):
            output_size = os.path.getsize(output_file)
            print(f"\n{'=' * 70}")
            print(f"✓ SUCCESS! Subsetting completed")
            print(f"✓ Output file: {output_file}")
            print(f"✓ Output size: {output_size:,} bytes ({output_size / (1024**2):.2f} MB)")
            print(f"{'=' * 70}")
        else:
            print("\nError: Output file was not created.")
            sys.exit(1)
            
    except subprocess.CalledProcessError as e:
        print(f"\nError executing AWK command:")
        print(f"Command: {' '.join(cmd)}")
        print(f"Error: {e.stderr}")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        sys.exit(1)

def main():
    """Main function with argument parsing"""
    parser = argparse.ArgumentParser(
        description='Ultra-fast Beagle file subsetting using Python + AWK',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Keep only specified samples
  subset_beagle --input data.beagle.gz --keep samples.txt --out subset.beagle.gz
  
  # Remove specified samples (keep all others)
  subset_beagle --input data.beagle --remove bad_samples.txt --out filtered.beagle
  
  # Mix of compressed/uncompressed files
  subset_beagle --input input.beagle.gz --keep keep.txt --out output.beagle

Notes:
  - Input and output can be .beagle or .beagle.gz (automatically detected)
  - Sample list should be one sample ID per line, no header
  - Must specify either --keep or --remove (but not both)
  - AWK streaming provides optimal performance for large files
        """
    )
    
    parser.add_argument('--input', '-i', required=True,
                       help='Input Beagle file (.beagle or .beagle.gz)')
    parser.add_argument('--out', '-o', required=True,
                       help='Output Beagle file (.beagle or .beagle.gz)')
    
    # Mutually exclusive group for keep/remove
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--keep', '-k', metavar='FILE',
                      help='File with sample IDs to keep (one per line)')
    group.add_argument('--remove', '-r', metavar='FILE',
                      help='File with sample IDs to remove (one per line)')
    
    args = parser.parse_args()
    
    # Determine mode and sample list file
    if args.keep:
        sample_list_file = args.keep
        remove_mode = False
    else:  # args.remove
        sample_list_file = args.remove
        remove_mode = True
    
    # Run subsetting
    subset_beagle(args.input, sample_list_file, args.out, remove_mode)

if __name__ == "__main__":
    main()
