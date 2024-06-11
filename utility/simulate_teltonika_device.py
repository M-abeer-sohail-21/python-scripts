from re import findall
from os import listdir, path

class RawDataExtrator:
    base_path = './utility'

    def __init__(self, logs_folder_path, regex_pattern):
        
        self.logs_folder_path = logs_folder_path
        self.regex_pattern = regex_pattern
        dir_path = f'{self.base_path}/{logs_folder_path}'

        self._matches = []

        files =  [f'{dir_path}/{f}' for f in listdir(dir_path) if path.isfile(path.join(dir_path, f))]

        for f in files:
            with open(f, 'r') as ff:
                data = ff.read()
                self._matches.extend(findall(regex_pattern, data))
    
    @property
    def matches(self):
        return self._matches

    @property
    def count(self):
        return len(self._matches)

class TeltonikaCodec8Device:
    url = 'localhost'
    port = 57845
    hub_socket = f'{url}:{port}'

    regex_pattern = r"\\\"Raw\\\":\\\"([^\"]+)\\\""

    def __init__(self, imei):
        self._imei = imei
        self._data = RawDataExtrator(f'{self._imei[-4:]}-logs', self.regex_pattern).matches()



# print(RawDataExtrator(logs_folder_path, regex_pattern).count)

