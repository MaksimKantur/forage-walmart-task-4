import csv
import sqlite3

con = sqlite3.connect('shipment_database.db')
cursor = con.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS Spreadsheet0 (
    origin_warehouse TEXT,
    destination TEXT,
    product TEXT,
    on_time BOOLEAN,
    product_quantity INTEGER,
    driver_identifier TEXT
)
''')

with open('data/shipping_data_0.csv', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter=',', quotechar='|')
    next(reader)
    for row in reader:
        cursor.execute('INSERT INTO Spreadsheet0 VALUES (?, ?, ?, ?, ?, ?)', row)

cursor.execute('''
CREATE TABLE IF NOT EXISTS Spreadsheet12 (
    product TEXT,
    quantity INTEGER,
    origin TEXT,
    destination TEXT,
    on_time BOOLEAN,
    driver_identifier TEXT
)
''')

data1 = {}
with open('data/shipping_data_1.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        shipment_id = row['shipment_identifier']
        product = row['product']
        on_time = row['on_time']
        if shipment_id not in data1:
            data1[shipment_id] = {}
        if product not in data1[shipment_id]:
            data1[shipment_id][product] = {'quantity': 0, 'on_time': on_time}
        data1[shipment_id][product]['quantity'] += 1

data2 = {}
with open('data/shipping_data_2.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        ship_id = row['shipment_identifier']
        data2[ship_id] = {
            'origin_warehouse': row['origin_warehouse'],
            'destination': row['destination_store'],
            'driver_identifier': row['driver_identifier']
        }

for shipment_id, products in data1.items():
    if shipment_id in data2:
        origin = data2[shipment_id]['origin_warehouse']
        destination = data2[shipment_id]['destination']
        driver_identifier = data2[shipment_id]['driver_identifier']
        for product, details in products.items():
            cursor.execute('INSERT INTO Spreadsheet12 VALUES (?, ?, ?, ?, ?, ?)', (
                product, details['quantity'], origin, destination, details['on_time'], driver_identifier
            ))
    else:
        print(f"Shipment ID {shipment_id} not found in data2")

con.commit()
con.close()