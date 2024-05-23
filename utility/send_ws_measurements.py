from os import getenv
from dotenv import load_dotenv

load_dotenv()

# Edit here START ----------------------------------

endpoint = 'wslisteners/populateData'
backend = 'localhost'
token = os.getenv('C8Y_E2E_TOKEN')
base_path = './utility'
file_path = 'raw_c8y_measurements_to_send_to_wslistener.json'

# Edit here STOP -----------------------------------

