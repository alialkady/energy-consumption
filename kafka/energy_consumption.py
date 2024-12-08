import pandas as pd

# read csv file
data = pd.read_csv('HomeC.csv')

# get  columns needed
coulmns = ['use [kW]','gen [kW]', 'House overall [kW]' , 'Dishwasher [kW]', 'Furnace 1 [kW]', 'Furnace 2 [kW]', 'Home office [kW]', 'Fridge [kW]', 'Wine cellar [kW]', 'Garage door [kW]', 'Kitchen 12 [kW]', 'Kitchen 14 [kW]', 'Kitchen 38 [kW]' , 'Barn [kW]' , 'Well [kW]', 'Microwave [kW]', 'Living room [kW]','Solar [kW]']

data_filtered = data[coulmns]

# rename columns

coulmn_rename = {
                    'use [kW]': 'use',
                    'gen [kW]': 'gen',
                    'House overall [kW]': 'House overall',
                    'Dishwasher [kW]': 'Dishwasher',
                    'Furnace 1 [kW]': 'Furnace 1',
                    'Furnace 2 [kW]': 'Furnace 2',
                    'Home office [kW]': 'Home office',
                    'Fridge [kW]': 'Fridge',
                    'Wine cellar [kW]': 'Wine cellar',
                    'Garage door [kW]': 'Garage door',
                    'Kitchen 12 [kW]': 'Kitchen 12',
                    'Kitchen 14 [kW]': 'Kitchen 14',
                    'Kitchen 38 [kW]': 'Kitchen 38',
                    'Barn [kW]': 'Barn',
                    'Well [kW]': 'Well',
                    'Microwave [kW]': 'Microwave',
                    'Living room [kW]': 'Living room',
                    'Solar [kW]': 'Solar'
}
data_filtered = data_filtered.rename(columns = coulmn_rename)

print(data_filtered)
