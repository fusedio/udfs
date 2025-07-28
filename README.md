<h1 align="center">
  Fused Python UDFs
</h1>
<h3 align="center">
  Analytics to Action, Faster
</h3>
<br><br>

![alt text](https://fused-magic.s3.us-west-2.amazonaws.com/docs_assets/github_udfs_repo/readme_udf_explorer.png)

This repo is a public collection of Fused User Defined Functions (UDFs).

Fused is the glue layer that interfaces data platforms and data tools via a managed serverless API. With Fused, you can write, share, or discover UDFs which are the building blocks of serverless operations. UDFs are Python functions that turn into live HTTP endpoints that load their output into any tools that can call an API.

## Quickstart

### 1. Install Fused Python SDK

[![PyPI Version](https://img.shields.io/pypi/v/fused.svg)](https://pypi.python.org/pypi/fused)

The Fused Python SDK is available at [PyPI](https://pypi.org/project/fused/). Use the standard Python [installation tools](https://packaging.python.org/en/latest/tutorials/installing-packages/). UDFs this repo expect the most recent version.

```bash
pip install "fused[all]"
```

Refer to the [official documentation](https://docs.fused.io/python-sdk/#python-install) for more

### 2. Load a UDF into a workflow

This snippet shows how to import a UDF from this repo into a Python environment. The URL is of the directory that contains a UDF generated with Fused.

```python
import fused

udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/DuckDB_NYC_Example")
gdf = fused.run(udf=udf)
gdf
```

## Walkthrough

### Repo structure

This repository is structured to facilitate easy access of UDFs and their supporting files. Each UDF, like `Sample_UDF`, is contained within its own subdirectory within the `public` directory - along with its documentation, code, metadata, and utility function code.

Each UDF can be thought of as a standalone Python package.

```
├── README.md
└── public
    └── Sample_UDF
        ├── README.MD
        ├── Sample_UDF.py
        ├── meta.json
        └── utils.py
```

Files relevant to each UDF are:
- `README.md` Provides details of the UDF's purpose and how it works.
- `Sample_UDF.py` This eponymous Python file contains the UDF's business logic as a Python function decorated with `@fused.udf`.
- `meta.json` This file contains metadata needed to render the UDF in the Fused explorer and for the UDF to run correctly.
- `utils.py` This Python file contains helper functions the UDF (optionally) imports and references.



### Contribute a UDF

1. Save UDF

```python
import fused

@fused.udf
def my_udf(bounds: fused.types.Tile = None):
    import pandas as pd
    return pd.DataFrame({'Hello': ['from Fused']})

# Run locally
print(fused.run(my_udf))

# Save locally
my_udf.to_directory('my_udf')
# or for zip file: my_udf.to_file('my_udf.zip')

# Save remotely to Fused
my_udf.to_fused('my_udf')
```

"Save locally" generates the UDF folder on your local system, which you'll use in the following step.

2. Open a PR

Clone this repo to your local system and add the UDF folder under `public` or `community`. Create a PR on this repo.

## Ecosystem

Build any scale workflows with the [Fused Python SDK](https://docs.fused.io/python-sdk/) and [Workbench webapp](https://docs.fused.io/workbench/), and integrate them into your stack by [calling then via HTTP](https://docs.fused.io/core-concepts/run-udfs/run-small-udfs/#http-requests)

![alt text](https://fused-magic.s3.us-west-2.amazonaws.com/docs_assets/ecosystem_diagram.png)

## Documentation

Fused documentation is in [docs.fused.io](https://docs.fused.io/).

## Contribution guidelines

All UDF contributions, bug reports, bug fixes, documentation improvements, enhancements, and ideas are welcome.


### Pre commit

Please run pre-commit hooks on your UDF prior to submitting.

```
pre-commit install
pre-commit run --files public/DuckDB_NYC_Example/*
```

## License

[MIT License](./LICENSE)
