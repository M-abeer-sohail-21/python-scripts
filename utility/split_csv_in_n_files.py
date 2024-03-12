import csv
import os

def split_csv_file(input_file, output_dir, n):
    # Ensure the output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    with open(input_file, 'r') as infile:
        reader = csv.reader(infile)
        # Skip the header if it exists
        header = next(reader)
        lines = list(reader)
        total_lines = len(lines)
        lines_per_file = total_lines // n
        
        for i in range(n + 1):
            output_file = os.path.join(output_dir, f'split_{i+1}.csv')
            with open(output_file, 'w', newline='') as outfile:
                writer = csv.writer(outfile)
                writer.writerow(header)  # Write the header to the first line of each file
                start_index = i * lines_per_file
                end_index = 0
                if n == i:
                    end_index = None
                else:
                    end_index = (i +  1) * lines_per_file
                for line in lines[start_index:end_index]:
                    writer.writerow(line)

# Example usage
input_file = '/home/sarwan/Downloads/salesforce_analytics_hourly.csv'
output_dir = '/home/sarwan/Downloads/salesforce_analytics_hourly_split_csv_files'
n =  5  # Number of files to split into
split_csv_file(input_file, output_dir, n)
