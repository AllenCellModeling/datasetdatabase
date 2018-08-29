# DatasetDatabase

Dataset Database (dsdb) is a python package for handling arbitrary datasets with little to no similarity between any given one. Additionally, it provides mechanisms for versioning, deduplicating, validation, and tracking datasets as they are ingested, processed, or created. While you do not have to use the database portions of this package, it is highly encouraged when conducting experiments and analysis as the database will allow you to properly version and track your analysis as you go.


***Full Versioning, Deduplication, Validation, and Tracking of Arbitrary Datasets***
```python
ds = Dataset("data.csv")
db = DatasetDatabase(config=dsdb.LOCAL)

def generate_report(df, **kwargs):
    report = pd.DataFrame()
    ...
    return report

report = ds.process(generate_report, db)
```

```
Recent Datasets:
--------------------------------------------------------------------------------
    {DatasetId: 1, Name: "MitoEval20180807", ...}
    {DatasetId: 2, Name: "mito predictions", ...}
    {DatasetId: 3, Name: "mito predictions test set", ...}
```

![dataset network graph](./resources/dataset_graph.png)

---

There are three core functions for using a dataset database:

1. [Ingest](./ingest.md)
2. [Process](./process.md)
3. [Share](./share.md)

For a more detailed understanding of the package here is a full list of major topics that you can view for more detailed information.

* [Database Schemas](./schema.md)
* [Database Constructor](./constructor.md)
* [File Management Systems](./fms.md)
* [Processing and Algorithm Handling](./run.md)

This is a completely open source project created at the Allen Institute for Cell Science and contributions are welcome. Please refer to the [AllenCellModeling GitHub](https://github.com/allencellmodeling) for contributing, bug reports, and feature requests.

Distributed under MIT License.

**Last updated: 28 August 2018**
