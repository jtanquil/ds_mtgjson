# -*- coding: utf-8 -*-
"""
cleaned_data.py

This contains the processed version of the all_sets DataFrame and the all_cards DataFrame.
"""

import pandas as pd
import json


# read sets into a DataFrame; index is chosen as the orient so the set names are the indices
all_sets = pd.read_json("AllSets.json", orient = "index")


# the cards column contains the cards of each set in json format, so each set of cards can be
# converted from a json object into a DataFrame
all_sets.cards = all_sets.cards.apply(lambda x: pd.read_json(json.dumps(x), orient = "records"))


# setSize = number of cards in each set
# convert releaseDate to a datetime for easier use in time series analysis
all_sets["setSize"] = all_sets.apply(lambda x: x.cards.shape[0], axis = 1)
all_sets["releaseDate"] = pd.to_datetime(all_sets["releaseDate"])


# non-tournament legal sets
invalid_sets = ["UGL", "UNH", "UST", "pCEL", "pHHO", "VAN"]

# the resulting mapping returns all sets that aren't in invalid_sets
all_sets = all_sets.loc[~all_sets.code.map(lambda x: x in invalid_sets)]


# these are the card layouts for "typical" Magic cards - the rest are the layouts we need to remove
card_layouts = ["double-faced", "flip", "leveler", "meld", "normal", "split"]

# the outer lambda defines an indexing by location to apply to each element in the cards column, and the
# inner map/lambda defines that indexing as one that removes the given layouts/types
all_sets.cards = all_sets.cards.apply(lambda x: x.loc[x.layout.map(lambda y: y in card_layouts)])
all_sets.cards = all_sets.cards.apply(lambda x: x.loc[x.types.map(lambda y: y != ["Conspiracy"])])


# function that replaces variable power/toughness with NaN
def fix_pts(c):
    col_list = list(c.columns)
    
    # only apply this to cards with power/toughness
    # not enough to check for creature since some noncreature cards have power/toughness (vehicles)
    if "power" in col_list and "toughness" in col_list:
        c.loc[:, "power"] = pd.to_numeric(c.loc[:, "power"], errors = "coerce")
        c.loc[:, "toughness"] = pd.to_numeric(c.loc[:, "toughness"], errors = "coerce")
    
    return c
    
all_sets.cards = all_sets.cards.apply(fix_pts)


# these columns tend to be set-dependent - for instance, the same card can have different borders in different
# reprintings; all_cards is just a list of cards, so we remove these columns
cols_to_remove = ["multiverseid", "imageName", "border", "mciNumber", "foreignNames",
                  "originalText", "originalType", "source"]

# casting the columns as sets makes writing the loc much easier, there could be a computational cost though? not sure
all_sets.cards = all_sets.cards.apply(lambda x: x.loc[:, list(set(x.columns) - set(cols_to_remove))])


# we standardize the columns of each cards DataFrame by taking the set-theoretic union of the columns
# and appending the remaining columns to each DataFrame.
union_set = set()
set_cols = all_sets.cards.map(lambda x: set(x.columns))

# would like to find a non-iterative approach to this using .apply 
for setname in set_cols.index:
    union_set = union_set | set_cols[setname]


# this function takes a cards DataFrame and appends to it the remaining columns in union_set
def addcols(cards, union_set):
    unused_cols = union_set - set(cards.columns)
    new_cols = pd.DataFrame(data = None, index = cards.index, columns = list(unused_cols))
    return cards.join(new_cols)
    
# after appending the columns we sort them in alphabetical order to ensure the columns line up when the DataFrames are concatenated 
all_sets.cards = all_sets.cards.apply(lambda x: addcols(x, union_set))
all_sets.cards = all_sets.cards.apply(lambda x: x.reindex(sorted(list(x.columns)), axis = 1))


# the columns we want to retain in all_cards
all_cards_columns = ['names', 'layout', 'manaCost', 'cmc', 'colors', 'colorIdentity',
                    'supertypes', 'types', 'subtypes', 'text', 'power', 'toughness',
                    'loyalty', 'rulings', 'foreignNames', 'printings', 'legalities']


# set the index of all_cards to be the name column, so we can search cards by name
all_cards = pd.DataFrame(data = None, columns = all_cards_columns)
all_cards.rename_axis("name", inplace = True)


# this function takes a row of all_sets and performs the conversion on the DataFrame in cards.
# the entire row is needed because the convert_printings function needs the set code to create the dictionary.
def convert_row(row):
    row["cards"]["printings"] = row["cards"].apply(lambda x: {row["code"] : x["rarity"]}, axis = 1)
    row["cards"].set_index("name", inplace = True) # the name is a much more natural index than the default index, which was just the first column by alpha order
                                                              
    return row

only_cards = all_sets.apply(convert_row, axis = 1)["cards"]


# again, casting the columns as sets allows us to use set notation which simplifies this function
def filter_columns(row, all_cards_cols):
    set_cols = list(row.columns)
    intersection = list(set(set_cols) & set(all_cards_cols))
    
    return row.filter(intersection)

only_cards = only_cards.apply(lambda x: filter_columns(x, all_cards_columns))
all_cards = pd.concat(list(only_cards))


# merges a list of dictionaries together; assumes that the dictionaries have no keys in common
def merge_dicts(dicts):
    merged_dicts = {}
    
    for d in dicts:
        for k, v in d.items():
            merged_dicts.update({k : v})
    
    return merged_dicts


for cardname in all_cards.index.unique():
    reprints = all_cards.loc[cardname]
    
    # this checks that the DataFrame above actually has more than one card - if it had only one, then
    # reprints would instead be a Series where the attributes of the card are the rows
    if len(reprints.shape) > 1:
        merged_dicts = merge_dicts(list(reprints.printings))
        reprints.iat[0, list(reprints.columns).index("printings")].update(merged_dicts) # updates the first entry of the card with the completed dictionary
        
        
# for each reprinted card, the first reprint has the completed printing/rarity dictionary, so we can get rid of every other duplicate
all_cards = all_cards[~all_cards.index.duplicated(keep = "first")]


# the two conditions being checked here are:
# 1. one of the printings of each card is a non-tournament legal set
# 2. the card isn't a basic land (since those are printed in every set)
# this filters out all cards satisfying both conditions.
all_cards = all_cards.loc[~(all_cards.printings.map(lambda x: bool(set(invalid_sets) & set(x))) & all_cards.supertypes.map(lambda x: x != ["Basic"]))]


colorless = all_cards.loc[all_cards.colors.isnull()]
all_cards.loc[colorless.index, "colors"] = colorless.colors.apply(lambda x: [])


for col in ["power", "toughness", "loyalty"]:
    all_cards.loc[:, col] = all_cards.loc[:, col].infer_objects()