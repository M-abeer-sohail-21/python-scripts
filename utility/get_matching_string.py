import re
from ignore_constants import *

def find_matches(pattern, text):
    # The pattern matches "Bike" followed by exactly 5 digits
    matches = re.findall(pattern, text)
    # Convert the matched strings to integers
    return [int(match) for match in matches]

# Example usage
pattern = r'Bike (\d{5})'
text = text
numbers = find_matches(pattern, text)
print('\n'.join([str(i) for i in numbers]))
