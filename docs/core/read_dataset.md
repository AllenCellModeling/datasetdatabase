<h1 id="datasetdatabase.core.read_dataset">read_dataset</h1>

```python
read_dataset(path:Union[str, pathlib.Path]) -> datasetdatabase.core.Dataset
```

Read a dataset using deserializers known to DatasetDatabase. This function
will properly handle ".csv" and our own ".dataset" files. Other support to
come.

#### Example
```
>>> read_dataset("path/to/data.csv")
{info: ...,
 ds: True,
 md5: "lakjdsfasdf9823h7yhkjq237",
 sha256: "akjsdf7823g2173gkjads7fg12321378bhfgasdf",
 ...}

>>> read_dataset("path/to/data.dataset")
{info: ...,
 ds: True,
 md5: "8hasdfh732baklhjsf",
 sha256: "87932bhksadkjhf78923klhjashdlf",
 ...}

```


#### Parameters
##### path: str, pathlib.Path
The filepath to read.


#### Returns
##### dataset: Dataset
The reconstructed and read dataset with it's database connection intact.


#### Errors
##### ValueError
The dataset could not be read.


