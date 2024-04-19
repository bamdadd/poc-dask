import random
import time
import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd

index = pd.date_range("2021-09-01", periods=2400, freq="1h")
df = pd.DataFrame({"a": np.arange(2400), "b": list("abcaddbe" * 300)}, index=index)
ddf = dd.from_pandas(df, npartitions=10)
ddf
print(ddf)

print(ddf["2021-10-01": "2021-10-09 5:00"].compute())

print(ddf.a.mean().compute())
print(ddf.b.unique().compute())

result = ddf["2021-10-01": "2021-10-09 5:00"].a.cumsum() - 100

result.dask

result.visualize()



def costly_simulation(l: list):
    print(f"running for {len(l)} items")
    time.sleep(random.randrange(0, 1))
    return len(l)

def costly_simulation2(l:list):
    print(f"running for {len(l)} items")
    time.sleep(random.randrange(0, 1))
    return len(l)


from dask.distributed import Client

if __name__ == '__main__':
    cluster = Client()  # Fully-featured local Dask cluster
    # cluster = LocalCluster()    # Simplified local Dask cluster
    # client = cluster.get_client()

    df = pd.DataFrame({"a": np.arange(2400), "b": list("abcaddbe" * 300)}, index=index)
    ddf = dd.from_pandas(df, npartitions=10)

    # Dask works as normal and leverages the infrastructure defined above
    ddf.a.sum().compute()
    lazy_results = []
    for i in range(0, 10, 5):
        lazy_results.append(dask.delayed(costly_simulation)([1, 2, 3, 4, 5]))

    futures = dask.persist(lazy_results)
    print(futures)
    print(dask.compute(futures))

    lazy_results2 = []
    for i in range(0, 10, 5):
        lazy_results2.append(dask.delayed(costly_simulation2)([1, 2, 3, 4, 5]))

    print(lazy_results2)
    futures2 = dask.persist(lazy_results2)
    print(futures2)
    print(dask.compute(futures2))

    while True:
        pass
