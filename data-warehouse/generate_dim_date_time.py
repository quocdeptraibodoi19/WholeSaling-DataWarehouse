import pandas as pd

# Define the start and end dates
start_date = pd.to_datetime('today')
end_date = pd.to_datetime('2050-12-31')

# Generate a date range with hourly frequency
date_range = pd.date_range(start=start_date, end=end_date, freq='H')

# Create a DataFrame with the date range as the index
df = pd.DataFrame(index=date_range)

# Add columns for year, month, day, hour, minute, second, weekday, week number, quarter, day of year, week ending date, month name, quarter name, season, holiday indicator, daylight saving time indicator, ISO week number, year-quarter, year-month
df["id"] = df.index
df['Date'] = df.index.date
df['Time'] = df.index.time
df['Year'] = df.index.year
df['Month'] = df.index.month
df['Day'] = df.index.day
df['Hour'] = df.index.hour
df['Minute'] = df.index.minute
df['Second'] = df.index.second
df['Weekday'] = df.index.day_name()
df['Week Number'] = df.index.isocalendar().week
df['Quarter'] = df.index.quarter
df['Day of Year'] = df.index.dayofyear
df['Week Ending Date'] = df.index + pd.tseries.offsets.Week(weekday=6)
df['Month Name'] = df.index.month_name()
df['Quarter Name'] = 'Q' + (df.index.quarter * 3).astype(str)
df['Season'] = df.index.month.map({1: 'Spring', 2: 'Spring', 3: 'Spring', 4: 'Spring', 5: 'Spring', 6: 'Summer', 7: 'Summer', 8: 'Summer', 9: 'Summer', 10: 'Autumn', 11: 'Autumn', 12: 'Autumn'})
df['ISO Week Number'] = df.index.isocalendar().week
df['Year-Quarter'] = df['Year'].astype(str) + '-Q' + df['Quarter'].astype(str)
df['Year-Month'] = df['Year'].astype(str) + '-' + df['Month'].astype(str).str.zfill(2)

# Save the DataFrame to a CSV file
df.to_csv('DateTime.csv', index=False)

print("CSV file 'dim_datetime_extended.csv' has been created.")
