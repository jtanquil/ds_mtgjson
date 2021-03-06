# Learning Data Science and Python with MTGJSON

This is a collection of jupyter notebooks that record my efforts to learn the basics of Data Science and Python using [MTGJSON](https://www.mtgjson.com), which contains information about Magic: the Gathering cards stored in JSON format. This is a dataset that, as a longtime Magic player, is near and dear to my heart, but is also an ideal data set to study for several reasons - there are a large number of observations (roughly 15,000 cards and over 200 sets released) while still being fairly tractable. The data set contains a variety of interesting characteristics, both numerical and qualitative, to analyze. Most importantly, one can use methods in statistical learning to explore interesting questions about Magic design and development, such as:

* Have creatures gotten more powerful over the years?
* Has removal gotten worse over the years?
* Given the rules text of a card, is it possible to predict its color and mana cost?

## Notebooks

[Data Cleaning](data_cleaning.ipynb) - loads the `AllCards.json` dataset and cleans up the data for analysis.