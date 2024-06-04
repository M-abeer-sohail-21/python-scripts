import folium
import webbrowser
from json import load
from datetime import datetime
import matplotlib.pyplot as plt

# Use revert = true with either/both of dateTo and dateFrom

path = './c8y/routes.json'
events = {}

with open(path, 'r') as f:
    events = load(f)

coords = [(x['c8y_Position']['lat'], x['c8y_Position']['lng']) for x in events['events']]

times = [datetime.strptime(x['time'], "%Y-%m-%dT%H:%M:%S.%fZ") for x in events['events']]

plt.figure(figsize=(10, 6))  # Optional: Adjusts the size of the plot
plt.scatter(times, [1]*len(times))  # Plot times against a constant value of 1
plt.xlabel('Time')  # Label for the x-axis
plt.ylabel('Value')  # Label for the y-axis
plt.title('Times vs Constant Value')  # Title for the plot
plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
plt.tight_layout()  # Adjust layout to prevent overlapping labels
plt.show()

# m = folium.Map(location=[events[0]['coords']['lat'], events[0]['coords']['lng']], zoom_start=15)

# # Loop through the events to add CircleMarkers and Popups
# for event in events[1:]:
#     folium.CircleMarker(
#         location=[event['coords']['lat'], event['coords']['lng']],
#         radius=5,
#         color='blue',
#         fill=True,
#         fill_color='blue'
#     ).add_to(m)
    
#     folium.Popup(f"Time: {event['time']}", max_width=250).add_to(m)

# # Save the map to an HTML file
# m.save("map.html")

# # Open the saved HTML file in a web browser
# webbrowser.open("map.html")
