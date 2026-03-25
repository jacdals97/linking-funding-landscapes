import pandas as pd
from pathlib import Path
import json



dataPath = Path('Data')
resultsPath = Path('Results')

rorDatasetPath = dataPath/"v1.58-2024-12-11-ror-data_schema_v2.json"
rorTypes = pd.read_json(rorDatasetPath)
# index by id
rorTypes.set_index('id', inplace=True)

parentNetwork = {}
# ID -> parent ID

for id, relationships in zip(rorTypes.index, rorTypes['relationships']):
    for relationship in relationships:
        if relationship['type'] == 'parent':
            parentNetwork[id] = relationship['id']


from collections import Counter

typesCounter = Counter()
for types in rorTypes['types']:
    for type in types:
        typesCounter[type] += 1/len(types)

ancestorTypes = []
for id in rorTypes.index:
    ancestorTypes.append([])
    while id in parentNetwork:
        id = parentNetwork[id]
        ancestorTypes[-1].extend(rorTypes.loc[id, 'types'])


rorTypes['ancestorTypes'] = ancestorTypes

# Only id, ancestorTypes, and types
rorTypesReduced = rorTypes[['ancestorTypes', 'types']]
# save to csv giving name id to index
rorTypesReduced.to_csv(resultsPath/"rorTypes_ancestor.csv", index=True)

