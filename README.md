# Running
Naviagate to the root of this repostory and install the dependencies
```
pip3 install pipenv==2022.5.2 && \

pipenv shell
pipenv install
```

Run backfill ingestion
```
python ingestion/disaggregated_futures/runner.py --start 2011 --end 2024
```


## Notes
* The dataset is quite wide. Depending on the use case it would probably make sense to not ingest all of it and hence bypass problems that may arise in columns that would never be needed/used.
* I opted to forgo pyspark because in my opinion it is best run in an isolated env that
* doesnt rely on any global Spark installation. I did not want to assume that
* the team has Docker installed/felt a virtual env would be sufficient for this
* exercise b) the dataset is small enough that we can run it without Spark (for now and for many years to come)
* However, I tried to write it in a way such that spark read/write methods for the pandas/pyarrow format would be a low-lift swap. 

## Some (non exhaustive) assumptions 

As per the website: 
> Beginning in 1998, Commitments of Traders grain data has been reported in contracts rather than bushels. Note that changes in commitments from the last reports in 1997 were not calculated for the 1998 grain reports.
I am assuming that the column `Contract_Units` refers to the number of bushels
per contract
