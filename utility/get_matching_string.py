import re

def find_matches(pattern, text):
    # The pattern matches "Bike" followed by exactly 5 digits
    matches = re.findall(pattern, text)
    # Convert the matched strings to integers
    print(matches)
    return [int(match) for match in matches]

# Example usage
pattern = r'Bike (\d{5})'
text = "Bike 12345 is in the garage. Bike 67890 is in the basement."
numbers = find_bike_numbers(text)
