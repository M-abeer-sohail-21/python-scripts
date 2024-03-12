import re

def process_file(input_filename, output_filename):
    with open(input_filename, 'r') as file:
        content = file.read()
    
    # Replace all newlines with commas
    content = content.replace('\n', ',')
    
    # Replace all instances of "PM" with "PM" followed by a newline
    content = re.sub(r'(AM|PM)', r'\1\n', content)
    
    # Write the processed content to the output file
    with open(output_filename, 'w') as output_file:
        output_file.write(content)

# Example usage:
input_file_names = ['device_list_raw_ec2.txt', 'device_list_raw_k8s.txt']
output_file_names = ['device_list_processed_ec2.txt', 'device_list_processed_k8s.txt']  # Replace with your filename

for i in range(len(input_file_names)):
    process_file(input_file_names[i], output_file_names[i])