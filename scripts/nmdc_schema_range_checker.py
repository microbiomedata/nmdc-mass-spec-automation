from linkml_runtime import SchemaView
import pandas as pd

schema_url = "https://raw.githubusercontent.com/microbiomedata/nmdc-schema/refs/heads/main/nmdc_schema/nmdc_materialized_patterns.yaml"

schema_view = SchemaView(schema_url)

class_names = list(schema_view.all_classes().keys())
class_names.sort()

# make dictionary of class names, with slots, ranges, and structured patterns
class_dict = {}
classes_with_id_slot = []
for c_name in class_names:
    if c_name.startswith("Functional"):
        print("here")
    class_dict[c_name] = {}
    ic = schema_view.induced_class(c_name)
    for ican, icav in ic.attributes.items():
        if icav.range in class_names:
            if icav.structured_pattern is not None:
                class_dict[c_name][ican] = {"range": icav.range, "structured_pattern": icav.structured_pattern['syntax']}
            else:
                class_dict[c_name][ican] = {"range": icav.range, "structured_pattern": None}
        if ican == "id":
            classes_with_id_slot.append(c_name)

# make each class into a dataframe and pull toegher
class_dfs = []
for c_name in class_dict.keys():
    if len(class_dict[c_name]) == 0:
        continue
    df = pd.DataFrame.from_dict(class_dict[c_name], orient='index')
    df['class'] = c_name
    df = df.reset_index()
    df = df.rename(columns={'index': 'slot'})
    df = df[['class', 'slot', 'range', 'structured_pattern']]
    class_dfs.append(df)
class_df = pd.concat(class_dfs, ignore_index=True)

# only keep rows where the range is in classes_with_id_slot
class_df = class_df[class_df['range'].isin(classes_with_id_slot)]
class_df = class_df[~class_df['class'].str.contains("Database")]

# arrange by range
class_df = class_df.sort_values(by=['range', 'class', 'slot'])

# save as markdown table
test = class_df.to_markdown(index=False)
with open("nmdc_schema_class_ranges.md", "w") as f:
    f.write(test)

class_df.to_csv("nmdc_schema_class_ranges.csv",index=False)