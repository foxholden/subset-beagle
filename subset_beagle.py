#!/usr/bin/env python3
"""
Quickly subset beagle files

Usage:
    subset_beagle --input <file> --keep <samples> --out <output>
    subset_beagle --input <file> --remove <samples> --out <output>
"""

import sys
import gzip
import os
import argparse
import subprocess
import tempfile
from typing import Set, List, Tuple

def read_sample_list(sample_list_file: str) -> Set[str]:
    """Read sample IDs from a text file"""
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
    """Read header line from Beagle file (handles gzipped files)"""
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
        header_line: Beagle Header
        sample_list: sample IDs to keep or remove
        remove_mode: If True, remove samples in list; if False, keep only samples in list
    
    Returns:
        Tuple of (column_indices, samples_kept, samples_removed)
    """
    fields = header_line.split('\t')
    
    # always keep marker, allele1, allele2)
    columns_to_keep = [1, 2, 3]
    
    all_samples = set()
    samples_kept = set()
    
    for i in range(3, len(fields), 3):
        if i < len(fields):
            sample_id = fields[i]
            all_samples.add(sample_id)
            
            should_keep = (sample_id not in sample_list) if remove_mode else (sample_id in sample_list)
            
            if should_keep:
                # Add all 3 columns for this sample
                columns_to_keep.extend([i+1, i+2, i+3])
                samples_kept.add(sample_id)
    
    samples_removed = all_samples - samples_kept
    
    # validation
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
        columns: List of column indices to keep (1-indexed)
        input_file: Input Beagle
        output_file: Outfile path
        use_progress: If True and pv is available, show progress bar
    
    Returns:
        Command as list of strings for subprocess
    """
    # format: print $1, $2, $3, ... sep = "\t"
    column_refs = ','.join([f'${col}' for col in columns])
    awk_script = f'{{OFS="\\t"; print {column_refs}}}'
    
    # determine if files are gzipped
    input_is_gz = input_file.endswith('.gz')
    output_is_gz = output_file.endswith('.gz') or output_file.endswith('.beagle.gz')
    
    # get file size for progress bar
    file_size = os.path.getsize(input_file)
    
    # check if pv is available
    has_pv = use_progress and check_pv_available()
    
    # build pv command if available
    if has_pv:
        pv_cmd = f"pv -p -t -e -r -b -s {file_size}"
    else:
        pv_cmd = "cat"
    
    # build command pipeline
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
    Main function
    
    Args:
        input_file: Path to input Beagle
        sample_list_file: Path to sample list
        output_file: Path to outfile
        remove_mode: If True, remove samples; if False, keep samples
    """
    print("=" * 70)
    print("SUBSETTING BEAGLE")
    print("=" * 70)
    
    # infile present?
    if not os.path.exists(input_file):
        print(f"Error: Input file '{input_file}' not found.")
        sys.exit(1)
    
    # add .beagle extension
    if not output_file.endswith('.beagle') and not output_file.endswith('.beagle.gz'):
        output_file = output_file + '.beagle'
    
    # read sample list
    print(f"\n[1/4] Reading sample list from: {sample_list_file}")
    sample_list = read_sample_list(sample_list_file)
    print(f"      Found {len(sample_list)} samples in list")
    
    # read header
    print(f"\n[2/4] Reading header from: {input_file}")
    header_line = read_header(input_file)
    total_samples = (len(header_line.split('\t')) - 3) // 3
    print(f"      Total samples in file: {total_samples}")
    
    # get cols to keep
    print(f"\n[3/4] Calculating columns to keep...")
    columns_to_keep, samples_kept, samples_removed = find_columns_to_keep(
        header_line, sample_list, remove_mode
    )
    
    # show results
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
    
    # generate and execute AWK command
    print(f"\n[4/4] Processing file...")
    print(f"      Input:  {input_file}")
    print(f"      Output: {output_file}")
    
    cmd, has_pv = generate_awk_command(columns_to_keep, input_file, output_file)
    
    if has_pv:
        print(f"\n      Progress (% data processed, time elapsed, rate):")
    else:
        print(f"\n      Note: Install 'pv' for progress bar (conda install pv or apt install pv)")
        print(f"      Processing...")
    
    try:
        # execute AWK command
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        if has_pv:
            for line in process.stdout:
                print(f"      {line}", end='', flush=True)
        
        # wait to complete
        return_code = process.wait()
        
        if return_code != 0:
            print(f"\nError: AWK command failed with return code {return_code}")
            sys.exit(1)
        
        # check if outfile was created
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
    """Main function and parse args"""
    parser = argparse.ArgumentParser(
        description='Subset Beagle File',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--input', '-i', required=True,
                       help='Input Beagle file (.beagle or .beagle.gz)')
    parser.add_argument('--out', '-o', required=True,
                       help='Output Beagle file (.beagle or .beagle.gz)')
    
    # mutually exclusive group for keep/remove
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--keep', '-k', metavar='FILE',
                      help='File with sample IDs to keep (one per line)')
    group.add_argument('--remove', '-r', metavar='FILE',
                      help='File with sample IDs to remove (one per line)')
    
    args = parser.parse_args()
    
    # determine mode and sample list
    if args.keep:
        sample_list_file = args.keep
        remove_mode = False
    else:  # args.remove
        sample_list_file = args.remove
        remove_mode = True
    
    # run subsetting
    subset_beagle(args.input, sample_list_file, args.out, remove_mode)

if __name__ == "__main__":
    main()
