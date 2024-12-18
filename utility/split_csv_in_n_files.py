import csv
import os

def split_csv_file(input_file, output_dir):
    # Ensure the output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    with open(input_file, 'r') as infile:
        reader = csv.reader(infile)
        # Skip the header if it exists
        header = next(reader)
        lines = list(reader)
        total_lines = len(lines)

        print('Total number of lines:', total_lines)

        n = 0
        ready_to_go = ''

        while True:
            n = int(input('Enter your desired value of n: '))

            lines_per_file = total_lines // n
            remaining_lines = total_lines - lines_per_file * n

            print('Lines per file:', lines_per_file, 'Remaining lines:', remaining_lines)

            while True:
                ready_to_go = input('Continue? (y/n): ')

                if ready_to_go not in ['y', 'n']:
                    print('Choose from (y/n)')
                else:
                    break
            
            if ready_to_go == 'y':
                break
            
        for i in range(n + 1):
            output_file = os.path.join(output_dir, f'{output_dir}_split_{i+1}.csv')
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
# -------------------------------------- START to edit here -----------------------------------------------
input_file = '/home/sarwan/Downloads/salesforce-analytics.csv'
output_dir = '/home/sarwan/Downloads/salesforce-analytics'
# -------------------------------------------- END --------------------------------------------------------
split_csv_file(input_file, output_dir)