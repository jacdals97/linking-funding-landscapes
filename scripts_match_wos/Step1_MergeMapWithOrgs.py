# %% [markdown]
# Jin's homework: explore the dataset 
# Due Feb 13 

# %%
# load the datafile and print the first few rows
import pandas as pd
# feather is not needed, using pandas to read feather files
import os
import pandas as pd

# INPUTS
wosprocessedfile = "Data/maps2wos_duplicates-20251023.parquet"
jins_datafile = "Data/orgs.feather"

# OUTPUTS
mergedMapOrgsFile = "Data/wos2jins_2023_duplicates-20251023.feather"




# %%


# %%
# read the wos processed data

wosprocessed = pd.read_parquet(wosprocessedfile)
jins_data = pd.read_feather(jins_datafile)

jins_data.keys()




# %%


# %%
# merge the two dataframes by coloumn sid
merged = pd.merge(wosprocessed, jins_data, on="sid", how="left")


# %%
merged.dropna(subset=["sid"])

# %%
# create a new general id column (gid). if we have an "id" not nan, we use OA_<id>, otherwise we use sid
gids = []
for oid, sid in zip(merged["id"], merged["sid"]):
    if oid==oid and oid:
        gids.append("OA_" + str(int(oid)))
    else:
        gids.append(sid)
merged["gid"] = gids
# print the first few rows



# %%


# %%
merged.to_feather(mergedMapOrgsFile)
