from sys import argv
from datetime import datetime
from functools import partial

import pandas as pd
import scipy as sp
Series, DataFrame = pd.Series, pd.DataFrame

if len(argv) < 2:
    print 'Supply the path of raw data files as first argument'
    exit(1)

PATH = argv[1]

read_csv = partial(pd.read_csv, nrows=None, sep='^',
                   quotechar='~', header=None)
blank_string = lambda s: '' if str(s) == 'nan' else s

# read foods list
foods = read_csv(open(PATH + '/FOOD_DES.txt', 'r'),
                 names=['food_code', 'food_group_code', 'food_name', '3', 'common_name',
                        'manufacturer', '6', '7', '8', 'sci_name', '10', '11', '12', '13'],
                 usecols=['food_code', 'food_group_code', 'food_name', 'common_name',
                          'manufacturer', 'sci_name'])
foods['common_name'] = foods['common_name'].map(blank_string)
foods['manufacturer'] = foods['manufacturer'].map(blank_string)
foods['sci_name'] = foods['sci_name'].map(blank_string)

# extend food list with group names
food_group = read_csv(open(PATH + '/FD_GROUP.txt', 'r'),
                      names=['food_group_code', 'food_group'])
foods = pd.merge(foods, food_group,
                 how='inner', on='food_group_code')
del food_group
del foods['food_group_code']

# read footnotes
footnote = read_csv(open(PATH + '/FOOTNOTE.txt', 'r'),
                    names=['food_code', 'footnote_code', 'type', 'nutrient_code', 'footnote'],
                    index_col=['food_code', 'nutrient_code', 'type'])
footnote = footnote['footnote'].dropna()

# read nutrient list
nutrients = read_csv(open(PATH + '/NUTR_DEF.txt', 'r'),
                     names=['nutrient_code', 'unit', 'nutrient_tag', 'nutrient', '4', '5'],
                     usecols=['nutrient_code', 'unit', 'nutrient_tag', 'nutrient'])
nutrients['unit'] = nutrients['unit'].map(lambda s: s.decode('iso-8859-15').encode('utf-8'))
nutrients['nutrient_tag'] = nutrients['nutrient_tag'].map(blank_string)

# read nutrient <--> food data
nutrient_food = read_csv(open(PATH + '/NUT_DATA.txt', 'r'),
                         names=['food_code', 'nutrient_code', 'value', '3', '4', 'source_code',
                                'derivation_code', '7', '8', '9', '10', '11', '12', '13', '14',
                                '15', 'updated', '17'],
                         usecols=['food_code', 'nutrient_code', 'value', 'source_code',
                                 'derivation_code', 'updated'])
nutrient_food['updated'] = nutrient_food['updated'].map(
    lambda d: pd.tslib.NaT if str(d) == 'nan' else datetime.strptime(str(d), '%m/%Y'))

# extend with source and derivation descriptions
deriv_desc = read_csv(open(PATH + '/DERIV_CD.txt', 'r'),
                      names=['derivation_code', 'derived_desc'])
nutrient_food = pd.merge(nutrient_food, deriv_desc, how='left', on='derivation_code')
del deriv_desc
del nutrient_food['derivation_code']
nutrient_food['derived_desc'] = nutrient_food['derived_desc'].map(blank_string)

source_desc = read_csv(open(PATH + '/SRC_CD.txt', 'r'),
                       names=['source_code', 'source'])
nutrient_food = pd.merge(nutrient_food, source_desc, how='left', on='source_code')
del source_desc
del nutrient_food['source_code']

# read weights
weights = read_csv(open(PATH + '/WEIGHT.txt', 'r'),
                   names=['food_code', 'item', 'amount', 'size', 'weight', '5', '6'],
                   index_col=['food_code', 'item'],
                   usecols=['food_code', 'item', 'amount', 'size', 'weight'])

# reset indices
foods.set_index('food_code', inplace=True)
nutrients.set_index('nutrient_code', inplace=True)
nutrient_food.set_index(['food_code', 'nutrient_code'], inplace=True)

# store in hdf5
store = pd.HDFStore('nutrients.h5', complib='lzo', complevel=9)
store['weights'] = weights
store['footnote'] = footnote
store['foods'] = foods
store['nutrients'] = nutrients
store['nutrientdata'] = nutrient_food
store.close()
