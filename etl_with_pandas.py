import urllib.request
from zipfile import ZipFile
import pandas as pd

#replace just_another_bucket_name by a valid bucket name…
bucket = "angelo-datalake"

# download MovieLens 1M Dataset
print("downloading file from movielens website...")
urllib.request.urlretrieve(
        'http://files.grouplens.org/datasets/movielens/ml-1m.zip',
        '/tmp/ml-1m.zip')

# extract the zip file
print("extracting dataset into tmp folder...")
with ZipFile('/tmp/ml-1m.zip', 'r') as zipObj:
   zipObj.extractall('/tmp/')

# read the csv
print("reading csv files...")
movies_df = pd.read_csv("/tmp/ml-1m/movies.dat", "::", 
                        engine='python', 
                        header=None, 
                        names=['movieid', 'title', 'genres']) 
print("movies_df has %s lines" % movies_df.shape[0])
ratings_df = pd.read_csv("/tmp/ml-1m/ratings.dat", "::", 
                         engine='python', 
                         header=None, 
                         names=['userid', 'movieid', 'rating', 'timestamp']) 
print("ratings_df has %s lines" % ratings_df.shape[0])

# join both dataframes
print("merging dataframes...")
merged_df = pd.merge(movies_df, ratings_df, on='movieid')

# aggregate data from dataframes, counting votes...
print("aggregating data...")
aggregation_df = merged_df.groupby('title').agg({'rating': ['count', 'mean']})
aggregation_df.columns = aggregation_df.columns.droplevel(level=0)
aggregation_df = aggregation_df.rename(columns={
    "count": "rating_count", "mean": "rating_mean"
})

# sorting data and filtering only movies with more than 1000 votes...
print("sorting data...")
aggregation_df = aggregation_df.sort_values(
        'rating_mean', 
        ascending=False).loc[aggregation_df['rating_count'] > 1000].head()

# writing data...
print("writing file to s3...")
aggregation_df.to_parquet(
        "s3://" + 
        bucket + 
        "/data/processed/best_movies/best_movies.parquet.snappy")

# reading data...
print("reading file from s3 and printing result...")
result_df = pd.read_parquet(
        "s3://" + 
        bucket + 
        "/data/processed/best_movies/best_movies.parquet.snappy")
print("result_df has %s lines" % result_df.size)

print("Best rated movie is: ")
print(result_df[0:1])