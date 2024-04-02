import re
from ignore_constants import *

def find_matches(pattern, text, sort=False):
    # The pattern matches "Bike" followed by exactly 5 digits
    matches = re.findall(pattern, text)
    # Convert the matched strings to integers

    if sort:
        matches = sorted(matches)

    return matches

# Example usage
pattern = r'Bike (\d{5})' # r"Bike\s\d{5}\s\((?P<digits>\d{15})"

numbers = find_matches(pattern, text, True)
print('\n'.join(numbers))