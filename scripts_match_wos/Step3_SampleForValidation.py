import pandas as pd
from pathlib import Path 
from tqdm.auto import tqdm
import numpy as np
import csv
import seaborn as sns


# GENERAL PATHS
wosPath = Path("/mnt/sciencegenome/WOS_TSV_2023")
dataPath = Path('Data')
resultsPath = Path('Results')

oaInstitutionsPath = dataPath/"openalex_institutions"/"institutions_core+basic+alternate_names+ids+geo+urls+roles+metrics.tsv"
oaFundersPath = dataPath/"openalex_institutions"/"funders_core+basic+ids+extra+counts.tsv"

# INPUT
matchesPath = resultsPath/"grant_institutions_jin_2023_affiliation_match_20251023.parquet"
WOSDataPath = wosPath/"WoS_2023.tsv"

dfMatches = pd.read_parquet(matchesPath)


# OUTPUT 
outputPath = Path("Results")/"Samples" # ./ = current directory

outputBaseName = matchesPath.stem.replace("grant_agency_matched", "validation_dataset")


sampleByPaperCSVPath = outputPath/f"sample_by_paper_{outputBaseName}.csv"
sampleByAgencyCSVPath = outputPath/f"sample_by_agency_{outputBaseName}.csv"
completeCSVOutputPath = outputPath/f"complete_{outputBaseName}.csv"
completeOutputPath = outputPath/f"complete_{outputBaseName}.feather"


#----
oaInstitutions = pd.read_csv(oaInstitutionsPath, sep="\t", low_memory=False, quoting=csv.QUOTE_NONE, doublequote=False, escapechar='\\',dtype={"id":str})
oaFunders = pd.read_csv(oaFundersPath, sep="\t", low_memory=False, quoting=csv.QUOTE_NONE, doublequote=False, escapechar='\\',dtype={"id":str})

# oa = openalexraw.OpenAlex(openAlexPath=)

# need to eval entries in oaInstitutions["display_name_acronyms"]
import ast


oaID2Names = {}
oaID2Types = {}
for id, display_name, display_name_acronyms, display_name_alternatives in zip(
        oaInstitutions['id'].to_list(),
        oaInstitutions['display_name'].to_list(),
        oaInstitutions['display_name_acronyms'].to_list(),
        oaInstitutions['display_name_alternatives'].to_list()
    ):
    if id not in oaID2Names:
        oaID2Names[id] = []
    oaID2Names[id].append(display_name)
    display_name_acronyms = ast.literal_eval(display_name_acronyms)
    oaID2Names[id].extend(display_name_acronyms)
    display_name_alternatives = ast.literal_eval(display_name_alternatives)
    oaID2Names[id].extend(display_name_alternatives)
    oaID2Types[id] = "institutions"


for id, display_name, display_name_alternatives in zip(
        oaFunders['id'].to_list(),
        oaFunders['display_name'].to_list(),
        oaFunders['alternate_titles'].to_list()
    ):
    if id not in oaID2Names:
        oaID2Names[id] = []
    oaID2Names[id].append(display_name)
    display_name_alternatives = ast.literal_eval(display_name_alternatives)
    oaID2Names[id].extend(display_name_alternatives)
    oaID2Types[id] = "funders"


# turn into strings
oaID2Names = {"OA_"+k: "; ".join(v) for k, v in oaID2Names.items()}
oaID2Types = {"OA_"+k: v for k, v in oaID2Types.items()}

dfMatches["display_names"] = dfMatches["gid"].map(oaID2Names)
# for the null values, fill with "name"
# dfMatches["display_names"] = dfMatches["display_names"].fillna(dfMatches["grant_agency_normalized_matched"])
dfMatches["display_names"] = dfMatches["display_names"].fillna("NOT MATCHED")
# Add a column with all the openalex ID URLs to funders or institutions
# dfmatches["url_ids"] = dfMatches["gid"].apply(lambda x: "https://openalex.org/" + x if pd.notna(x) else "")
# url = https://openalex.org/<type>/<first letter of type><id>
def formatURL(x):
    if pd.isna(x):
        return ""
    oaType = oaID2Types.get(x, "")
    if oaType:
        firstLetter = oaType[0]
        return f"https://openalex.org/{oaType}/{firstLetter}{x[3:]}"
    else:
        return None
    
dfMatches["url_ids"] = dfMatches["gid"].apply(formatURL)


dfMatchesWithDisplayNames = dfMatches[dfMatches["display_names"].notna() & (dfMatches["display_names"] != "")]

allUTs = set(dfMatchesWithDisplayNames["UT"].unique())


uid2DisplayNames = {}
for uid, display_names in zip(dfMatchesWithDisplayNames["UT"], dfMatchesWithDisplayNames["display_names"]):
    if uid not in uid2DisplayNames:
        uid2DisplayNames[uid] = []
    uid2DisplayNames[uid].append(display_names)
# do the same for urls

uid2URLs = {}
for uid, url in zip(dfMatchesWithDisplayNames["UT"], dfMatchesWithDisplayNames["url_ids"]):
    if(not url or pd.isna(url) or url == ""):
        continue
    if uid not in uid2URLs:
        uid2URLs[uid] = []
    uid2URLs[uid].append(url)
# keep only unique URLs
uid2URLs = {uid: list(set(urls)) for uid, urls in uid2URLs.items()}

chunksize = 5000
length= 87162157

allUIDS = []
allDois = []
allTitles = []
allDisplayNames = []
allURLs = []
with tqdm(total=length, desc="Reading file ") as bar:
    for i, chunk in enumerate(pd.read_csv(WOSDataPath, chunksize=chunksize,sep="\t",encoding='utf-8',on_bad_lines="warn", low_memory=False,quoting=csv.QUOTE_NONE)):
        # only keep entries with non nan or non empty DI
        filteredChunk = chunk[chunk["DI"].notna() & (chunk["DI"] != "")]
        indicesOk = []
        for index, ut in enumerate(filteredChunk["UT"]):
            if ut in allUTs:
                indicesOk.append(index)
        filteredChunk = filteredChunk.iloc[indicesOk]
        if not filteredChunk.empty:
            uids = filteredChunk["UT"].values
            titles = filteredChunk["TI"].values
            dois = filteredChunk["DI"].values
            
            # transform dois into links by adding https://doi.org/
            dois = ["https://doi.org/" + doi if doi else "" for doi in dois]
            display_names = [" || ".join(uid2DisplayNames.get(uid, [""])) for uid in uids]
            # if there are multiple URLs, join them with ||
            urls = [" || ".join(uid2URLs.get(uid, [""])) for uid in uids]
            allUIDS.extend(uids)
            allTitles.extend(titles)
            allDois.extend(dois)
            allURLs.extend(urls)
            allDisplayNames.extend(display_names)

        bar.update(chunksize)




# Create a DataFrame with the results
dfResults = pd.DataFrame({
    "UT": allUIDS,
    "title": allTitles,
    "DOI": allDois,
    "display_names": allDisplayNames,
    "OA_URLs": allURLs
})

# Replace invalid surrogates in all string columns
for col in dfResults.select_dtypes(include="object"):
    dfResults[col] = dfResults[col].astype(str).str.encode("utf-8", "surrogatepass").str.decode("utf-8", "ignore")

# Save the DataFrame to a feather file
dfResults.to_feather(completeOutputPath)

# Save the DataFrame to a CSV file
dfResults.to_csv(completeCSVOutputPath, index=False, quoting=csv.QUOTE_NONNUMERIC)

# sample 1000 entries from DataFrame
dfSample = dfResults.sample(n=1000, random_state=42)
# Save the sample to a CVS file
dfSample.to_csv(sampleByPaperCSVPath, index=False, quoting=csv.QUOTE_NONNUMERIC)

# another sample by agency
uniqueAgencies = dfMatchesWithDisplayNames["grant_agency"].unique()
sampleUniqueAgencies = set(np.random.choice(uniqueAgencies, size=1000, replace=False))
dfSampleByAgency = dfMatchesWithDisplayNames[dfMatchesWithDisplayNames["grant_agency"].isin(sampleUniqueAgencies)]
# now pick 1 entry per agency
dfSampleByAgency = dfSampleByAgency.groupby("grant_agency").first().reset_index()
# get unique UTs
sampleByAgencyUTs = set(dfSampleByAgency["UT"].unique())

# dfResults but only with the sampled UTs
dfSampleByAgencyResults = dfResults[dfResults["UT"].isin(sampleByAgencyUTs)]
# Save the sample to a CSV file
dfSampleByAgencyResults.to_csv(sampleByAgencyCSVPath, index=False, quoting=csv.QUOTE_NONNUMERIC)
