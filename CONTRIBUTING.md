# Fused UDFs - Contributing Guide

Welcome to Fused! If you are reading this guide you are about to start creating your first UDF and start contributing to the Community UDFs . This guide highlights all the best practices to create an impactful UDF . Lets Dive in information.

## Table of contents

[TOC]

### Example UDFs for Reference

These are some of our most loved UDFs created by the community:

<table>
  <tr>
    <th>#</th>
    <th>UDF</th>
    <th>Description of the UDF</th>
    <th>Tags</th>
  </tr>
  <tr>
    <td>1</td>
    <td><a href="Flood Risk Analysis">Flood Risk Analysis</a></td>
    <td><ul>
          <li>It then fetches LULC data for the given bounding box and year, transforms it into vector geometries, and creates a GeoDataFrame representing water bodies. </li>
        </ul>
    </td>
    <td>Flood, Environment, Urban Planning</td>
  </tr>
  <tr>
    <td>2</td>
    <td><a href="H3 Hexagon Layer Example">H3 Hexagon Layer Example</a></td>
    <td><ul>
          <li>This UDF shows how to open NYC yellow taxi trip dataset using DuckDB and aggregate the pickups using H3-DuckDB. Results are visualized as hexagons.</li>
        </ul>
    </td>
    <td>H3 , Hexagon, NYC.</td>
  </tr>
  
</table>

Explore more UDFs for your usecases or develop one from your source in the workbench

## 1. Join the community

- **Discord** - Our Discord community is excellent and we encourage you to share your progress on the **General** channel.

## 2. Create the UDF

- **ask-for-help** - Post your doubts and stoppers on the as-got-help section of the Discord and we will do our best to solve your problems regarding building of the UDF
- **Documentation** - Please go through our extensive documentation of all the functions on the documentation.

### The UDF Style Guide

Each UDF is expected to follow certain style and patterns

- **Imports**:
  - Always put imports at the top of the file, just after any module comments and docstrings, and before module globals and constants.
  - Avoid wildcard imports (`from <module> import *`).
  - Use 4 spaces per indentation level.
- **Comments**:

  - Use comments to explain the code, especially complex logic. Refer to the [PEP 8 guidelines on comments](https://peps.python.org/pep-0008/#comments).

- **Type Hinting**:

  - Type hinting is encouraged to make the code more readable and maintainable. Refer to the [Python documentation on type hinting](https://docs.python.org/3/library/typing.html) for more details.

  _Example:_

  ```python
  def udf(param1: int, param2: str) -> str:
  ```

### Data Sources

Ensure that all the external data sources being used are either from pubclicly available sources or uploaded to the disk from the Fused Upload File section.

## 3. Get feedback and Ask for Help

Keep Sharing your progress on our Discord general channel and get feedback from us and other community members.

Post your doubts and stoppers on the ask-for-help section of the Discord and we will do our best to solve your problems regarding building of the UDF

## 4. Configure the Settings

Configure the settings in the workbench to set the default parameters, Default View State, Image previews and appropriate tags to make the UDF searchable and visually appealing in the catalog section.

## 4. Add a README.md

In the description, Describe your UDF and the data sources and add the thumbnail.

## 5. Pre commit

Please run pre-commit hooks on your UDF prior to submitting.

```
pre-commit install
pre-commit run --files /*Your UDF Folder*/*
```

## 6. Submit a Pull request

The purpose of this phase is to check that your Markdown is formatted correctly for any UDF that you submit

run a git pre commit on it
