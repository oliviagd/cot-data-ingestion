# Running
Naviagate to the root of this repostory and install the dependencies
```
pip3 install pipenv==2022.5.2

pipenv shell
pipenv install
```

Ingest data from the previous 5 several years
```
python ingestion/disaggregated_futures/runner.py --start 2015 --end 2024
```


## Notes
* The dataset is quite wide. If its possible to avoid maintaining a schema for 190+ columns, that would be desired. So depending on the use case it would probably make sense to not ingest all of it and hence bypass problems that may arise in columns that would never be needed/used.
  - The data ingestion works properly for years 2015 forward: there are some typing problems that show up in earlier years that need to be accounted for by manually specifying dtypes
* I opted to forgo pyspark ingestion because in my opinion it is best run in an isolated env that doesnt rely on any global Spark installation. I did not want to assume that the team has Docker installed, and ultimately felt a virtual env would be sufficient for this exercise. Additionally, the dataset is small enough that we can run it without Spark (for now and for many years to come)
* ^ on that note, however, I tried to write it in a way such that spark read/write methods for the pandas/pyarrow format would be a low-lift swap. 

## Some (non exhaustive) assumptions 
* I assumed that it was desired to ingest the entire dataset. Ideally we would maintain a schema and only read in certain columns, or alternatively ingest the entire dataset as a variant type (can discuss further). In the interest of time I did not create a schema for all 190 columns, thus the data ingestion is only works from 2015 onward at this time. 
* As per the website: 
> Beginning in 1998, Commitments of Traders grain data has been reported in contracts rather than bushels. Note that changes in commitments from the last reports in 1997 were not calculated for the 1998 grain reports.

I am assuming that the column `Contract_Units` refers to the number of bushels
per contract. 
* Furthermore, I limited my plot output to those market/exchanges that included "wheat" in the name. I observed the following:
```
>>> df[df["market_and_exchange_names"].str.lower().str.contains("wheat")][['market_and_exchange_names','contract_units']].drop_duplicates().values
array([['WHEAT-SRW - CHICAGO BOARD OF TRADE',
        '(CONTRACTS OF 5,000 BUSHELS)'],
       ['BLACK SEA WHEAT FINANCIAL - CHICAGO BOARD OF TRADE',
        '50 Metric Tons'],
       ['WHEAT-HRW - CHICAGO BOARD OF TRADE',
        '(CONTRACTS OF 5,000 BUSHELS)'],
       ['WHEAT-HRSpring - MINNEAPOLIS GRAIN EXCHANGE',
        '(CONTRACTS OF 5,000 BUSHELS)']], dtype=object)
```
From above you can see that `BLACK SEA WHEAT FINANCIAL - CHICAGO BOARD OF TRADE` is reported in `50 Metric Tons`, which was unclear to me how it should be use. Thus, I omitted this exchange from the plots. 

To calculate the long, short, and net positions, I performed the following:
* filtered the dataframe for the wheat-related markets/exchanges, as determined from the `market_and_exchange_names` column (minus one exchange as noted above)
* extracted the `bushels_per_contract` value from the `contract_units` function
* multiplied the columns by `bushels_per_contract`
* aggregated the columns by the date
* computed the net position by taking the difference of the long and short position columns

The above approach I will be transparent in having limited knowledge in such reporting practices and will take feedback...!
