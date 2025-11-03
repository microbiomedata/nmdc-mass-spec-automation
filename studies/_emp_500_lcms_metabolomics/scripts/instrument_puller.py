"""
This script will read in the biosample metadata file (biosample_attributes.csv) and use it to map raw data files to biosample ids.

It will then clean up the output for a final mapping as an input to the metadata generation script.
"""
from nmdc_api_utilities.instrument_search import InstrumentSearch

import pandas as pd

instrument_search = InstrumentSearch()
instrument_records = instrument_search.get_records(
    all_pages=True)
# Convert the records to a DataFrame
instrument_df = pd.DataFrame(instrument_records)

# Add a flag column if its a repeat of a vendor: model combination
instrument_df['is_repeat_of_model_vendor'] = instrument_df.duplicated(subset=['vendor', 'model'], keep=False)

# remove type column
instrument_df = instrument_df.drop(columns=['type'])

# reorder columns: id, name, vendor, model, is_repeat_of_model_vendor
instrument_df = instrument_df[['id', 'name', 'vendor', 'model', 'is_repeat_of_model_vendor']]
# arrange rows with is_repeat_of_model_vendor first
instrument_df = instrument_df.sort_values(by='is_repeat_of_model_vendor', ascending=False)

# convert to markdown table
instrument_markdown = instrument_df.to_markdown(index=False)
print(instrument_markdown)

print("here")
