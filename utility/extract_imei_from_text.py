import re

def extract_imei(text):
    regex = r"Bike\s\d{5}\s\((?P<digits>\d{15})"
    matches = re.findall(regex, text)
    return matches

def print_list_elems(lst):
    print('List items',len(lst))
    print('\n'.join(lst))

with open('raw_text.txt', 'r') as file:
    # Read the entire content of the file into a variable
    text = file.read()
    print_list_elems(extract_imei(text))