import csv
from datetime import datetime, timedelta
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

#ASSUMPTIONS 
# - We assume we dont have intersecting tick interval data in different files


#Class to represent OHLCV bars
class OHLCV:
    def __init__(self, timestamp, open, high, low, close, volume):
        self.timestamp = timestamp
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume

    def __str__(self):
        return (f"Timestamp: {self.timestamp}, "
                f"Open: {self.open:.2f}, "
                f"High: {self.high:.2f}, "
                f"Low: {self.low:.2f}, "
                f"Close: {self.close:.2f}, "
                f"Volume: {self.volume:.2f}")

#File to read the csv (ignore the header)
def read_csv(file_path, delimiter=','):
    data = []
    with open(file_path, 'r', newline='') as file:
        csv_reader = csv.reader(file, delimiter=delimiter)
        for row in csv_reader:
            data.append(row)
    return data[1:]


#time interval
interval = '1s'
time_frame = ["2024-09-16 09:30:00.076","2024-09-17 11:04:57.306"]

begin,end = datetime.strptime(time_frame[0], '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime(time_frame[1], '%Y-%m-%d %H:%M:%S.%f')

time_delta  = timedelta(seconds=1)

#Generate the OHLCV bars and return a list of lists
def solution(file_path):
    res = []
    csv_data = read_csv(file_path)

    first_time = datetime.strptime(csv_data[0][0], '%Y-%m-%d %H:%M:%S.%f')
    curr_interval = first_time + time_delta
    curr_object = ""


    if first_time < begin or first_time > end:
        return []
    

    for row in csv_data:
        timestamp, price, size = row[0], row[1], int(row[2])
        if price:
            price = float(price)
        else:
            continue
    
        time = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')

        if not curr_object:
            curr_object = OHLCV(timestamp, price, price, price, price, size)

        if (time <= curr_interval):
            curr_object.volume += size
            # print(curr_object.volume,size)
            curr_object.high = max(curr_object.high, price)
            curr_object.low = min(curr_object.low, price)
            curr_object.close = price
        else:
            res.append(curr_object)
            curr_object = OHLCV(timestamp, price, price, price, price, size)
            curr_interval += time_delta
    return [[row.timestamp, row.open, row.high, row.low, row.close, row.volume] for row in res]



#Grab all csv files to read
files = [os.path.join('data', f) for f in os.listdir('data') if os.path.isfile(os.path.join('data', f))]

#Remove old output
file_exists = os.path.isfile('output.csv')
if file_exists:
    os.remove('output.csv')


#Process all the csvs in parallel
processed_dataframes = []     
with ThreadPoolExecutor(max_workers=12) as executor:  # Adjust max_workers as needed
    future_to_file = {executor.submit(solution, file_path): file_path for file_path in files}
    
    for future in as_completed(future_to_file):
        file = future_to_file[future]
        try:
            result = future.result()
            if result is not None:
                processed_dataframes += result
        except Exception as e:
            print(f"Exception occurred for file {file}: {e}")


#Write results of processing to output csv
with open('output.csv', 'w', newline='') as out:
    csv_writer = csv.writer(out)
    csv_writer.writerow(['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
    for row in processed_dataframes:
        csv_writer.writerow(row)


#Sort csv by timestamp
def sort_csv_by_timestamp(input_file, output_file):
    # Read the CSV file
    with open(input_file, 'r') as file:
        reader = csv.reader(file)
        header = next(reader)
        data = list(reader)

    # Sort the data by timestamp
    data.sort(key=lambda row: datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S.%f'))

    # Write the sorted data to a new CSV file
    with open(output_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerows(data)

sort_csv_by_timestamp('output.csv', 'output_sorted.csv')





