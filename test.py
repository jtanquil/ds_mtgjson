# coding: utf-8
get_ipython().run_line_magic('cd', 'ds_mtgjson')
import pandas as pd
all_sets = pd.read_json("AllSets.json", orient = "index")
all_sets.shape
all_sets.columns
all_sets.describe()
import json
all_sets.cards = all_sets.cards.apply(lambda x: pd.read_json(json.dumps(x), orient = "records"))
all_sets.cards["XLN"].head()
all_sets["setSize"] = all_sets.apply(lambda x: x.cards.shape[0], axis = 1)
all_sets.loc[:, all_sets["setSize"]].head()
all_sets.head()
all_sets["releaseDate"] = pd.to_datetime(all_sets["releaseDate"])
all_sets.loc[:, ["name", "releaseDate"]].sort_values(["releaseDate"])
