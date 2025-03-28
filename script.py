import polars as pl
import duckdb
import dask.dataframe as dd
from pyspark.sql import SparkSession
import time
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# File paths
formats = ["txt", "parquet", "delta", "iceberg", "orc"]
files = {
    "SF10": {fmt: f"/app/data_sets{'/formats' if fmt != 'txt' else ''}/{sf}{'_'+fmt if fmt != 'txt' else '.txt'}" for fmt in formats},
    "SF50": {fmt: f"/app/data_sets{'/formats' if fmt != 'txt' else ''}/{sf}{'_'+fmt if fmt != 'txt' else '.txt'}" for fmt in formats},
    "SF100": {fmt: f"/app/data_sets{'/formats' if fmt != 'txt' else ''}/{sf}{'_'+fmt if fmt != 'txt' else '.txt'}" for fmt in formats}
}

# Results storage
results = {tech: {fmt: [] for fmt in formats} for tech in ["Polars", "DuckDB", "Dask", "Spark"]}

# Benchmarking loop
for sf in files.keys():
    print(f"\nTesting {sf}...")
    for fmt in formats:
        file = files[sf][fmt]
        print(f"  Format: {fmt}")

        # Polars
        start = time.time()
        if fmt == "txt":
            df = pl.read_csv(file, separator=";", has_header=False, new_columns=["station", "temp"])
        else:
            df = pl.read_parquet(file) if fmt == "parquet" else pl.read_parquet(file)  # Adjust for other formats
        result = df.group_by("station").agg([pl.min("temp"), pl.avg("temp"), pl.max("temp")])
        elapsed = time.time() - start
        results["Polars"][fmt].append(elapsed)
        print(f"    Polars: {elapsed:.2f} s")

        # DuckDB
        start = time.time()
        con = duckdb.connect()
        query = f"SELECT station, MIN(temp), AVG(temp), MAX(temp) FROM '{file}' GROUP BY station"
        result = con.execute(query).fetchall()
        elapsed = time.time() - start
        results["DuckDB"][fmt].append(elapsed)
        print(f"    DuckDB: {elapsed:.2f} s")

        # Dask
        start = time.time()
        if fmt == "txt":
            df = dd.read_csv(file, sep=";", names=["station", "temp"])
        else:
            df = dd.read_parquet(file)
        result = df.groupby("station").agg({"temp": ["min", "mean", "max"]}).compute()
        elapsed = time.time() - start
        results["Dask"][fmt].append(elapsed)
        print(f"    Dask: {elapsed:.2f} s")

        # Spark
        start = time.time()
        spark = SparkSession.builder.appName("1BRC").getOrCreate()
        if fmt == "txt":
            df = spark.read.option("delimiter", ";").csv(file, schema="station STRING, temp FLOAT")
        elif fmt == "delta":
            df = spark.read.format("delta").load(file)
        elif fmt == "iceberg":
            df = spark.read.format("iceberg").load(file)
        elif fmt == "orc":
            df = spark.read.orc(file)
        else:
            df = spark.read.parquet(file)
        result = df.groupBy("station").agg({"temp": "min", "temp": "avg", "temp": "max"})
        result.collect()
        elapsed = time.time() - start
        results["Spark"][fmt].append(elapsed)
        print(f"    Spark: {elapsed:.2f} s")
        spark.stop()

# Visualization
sf_labels = ["SF10", "SF50", "SF100"]
for tech in results.keys():
    for fmt in formats:
        plt.plot(sf_labels, results[tech][fmt], marker="o", label=f"{tech} ({fmt})")
plt.xlabel("Scale Factor")
plt.ylabel("Execution Time (s)")
plt.title("Performance Across Technologies and Formats")
plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
plt.tight_layout()
plt.savefig("performance_graph.png")
plt.show()

# Bar chart for comparison
data = []
for tech in results.keys():
    for fmt in formats:
        for sf_idx, sf in enumerate(sf_labels):
            data.append({"Tech": tech, "Format": fmt, "SF": sf, "Time": results[tech][fmt][sf_idx]})
df_results = pd.DataFrame(data)
sns.catplot(x="SF", y="Time", hue="Format", col="Tech", kind="bar", data=df_results)
plt.savefig("bar_comparison.png")






























# import polars as pl
# import duckdb
# import dask.dataframe as dd
# from pyspark.sql import SparkSession
# import time
# import matplotlib.pyplot as plt

# # Les fichiers
# files = {
#     "SF10": "/app/data_sets/measurements_sf10.txt",
#     "SF50": "/app/data_sets/measurements_sf50.txt",
#     "SF100": "/app/data_sets/measurements_sf100.txt"
# }

# # Les resultats
# results = {"Polars": [], "DuckDB": [], "Dask": [], "Spark": []}

# # Query: MIN, AVG, MAX par station
# for sf, file in files.items():
#     print(f"\nTesting {sf}...")

#     # Polars
#     start = time.time()
#     df = pl.read_csv(file, separator=";", has_header=False, new_columns=["station", "temp"])
#     result = df.group_by("station").agg([pl.min("temp"), pl.avg("temp"), pl.max("temp")])
#     elapsed = time.time() - start
#     results["Polars"].append(elapsed)
#     print(f"Polars: {elapsed:.2f} s")

#     # DuckDB
#     start = time.time()
#     con = duckdb.connect()
#     result = con.execute(f"SELECT station, MIN(temp), AVG(temp), MAX(temp) FROM '{file}' GROUP BY station").fetchall()
#     elapsed = time.time() - start
#     results["DuckDB"].append(elapsed)
#     print(f"DuckDB: {elapsed:.2f} s")

#     # Dask
#     start = time.time()
#     df = dd.read_csv(file, sep=";", names=["station", "temp"])
#     result = df.groupby("station").agg({"temp": ["min", "mean", "max"]}).compute()
#     elapsed = time.time() - start
#     results["Dask"].append(elapsed)
#     print(f"Dask: {elapsed:.2f} s")

#     # Spark
#     start = time.time()
#     spark = SparkSession.builder.appName("1BRC").getOrCreate()
#     df = spark.read.option("delimiter", ";").csv(file, schema="station STRING, temp FLOAT")
#     result = df.groupBy("station").agg({"temp": "min", "temp": "avg", "temp": "max"})
#     result.collect()
#     elapsed = time.time() - start
#     results["Spark"].append(elapsed)
#     print(f"Spark: {elapsed:.2f} s")
#     spark.stop()

# # Visualisation
# sf_labels = ["SF10", "SF50", "SF100"]
# for tech, times in results.items():
#     plt.plot(sf_labels, times, marker="o", label=tech)
# plt.xlabel("Scale Factor")
# plt.ylabel("Temps d'exécution (s)")
# plt.title("Performance des Technologies Analytiques")
# plt.legend()
# plt.savefig("performance_graph.png")
# plt.show()