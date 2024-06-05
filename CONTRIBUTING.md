# Fused UDFs - Contributing Guide

Welcome to Fused! If you are reading this guide you are about to start creating your first UDF and start contributing to the Community UDFs. This guide highlights all the best practices to create an impactful UDF. Lets Dive in information.

## Table of contents

- [Example UDFs for Reference](#example-udfs-for-reference)
- [Join the Community](#1-join-the-community)
- [Create the UDF](#2-create-the-udf)
- [The UDF Style Guide](#the-udf-style-guide)
- [Data Sources](#data-sources)
- [Get Feedback and Ask for Help](#3-get-feedback-and-ask-for-help)
- [Configure the Settings](#4-configure-the-settings)
- [Pre-commit](#5-pre-commit)
- [Submit a Pull Request](#6-submit-a-pull-request)

### Example UDFs for Reference

These are some of our most loved UDFs created by the community:

<table>
  <tr>
    <th>#</th>
    <th>UDF</th>
    <th>Description of the UDF</th>
    <th>Tags</th>
    <th>Author</th>
  </tr>
 
  <tr>
    <td>1</td>
    <td><a href="H3 Hexagon Layer Example">H3 Hexagon Layer Example</a></td>
    <td><ul>
          <li>This UDF shows how to open NYC yellow taxi trip dataset using DuckDB and aggregate the pickups using H3-DuckDB. Results are visualized as hexagons.</li>
        </ul>
    </td>
    <td>H3, Hexagon, NYC.</td>
    <td>Isaac Brodsky</td>

  </tr>
   <tr>
    <td>2</td>
    <td><a href="Flood Risk Analysis">Flood Risk Analysis</a></td>
    <td><ul>
          <li>It then fetches LULC data for the given bounding box and year, transforms it into vector geometries, and creates a GeoDataFrame representing water bodies. </li>
        </ul>
    </td>
    <td>Flood, Environment, Urban Planning</td>
        <td>K_Njoroge</td>

  </tr>
    <tr>
    <td>3</td>
    <td><a href="https://github.com/fusedio/udfs/tree/contribute-md/community/fhk/data_center_location_model">Data Center Location Model</a></td>
    <td><ul>
          <li>Showing that it's possible to dynamically model the network design problem interactively.</li>

</ul>
</td>
<td> h3, data-center, pozibl, highspy optimization </td>
<td> Fabion Kauker </td>

  </tr>
      <tr>
    <td>4</td>
    <td><a href="https://github.com/fusedio/udfs/tree/contribute-md/community/samlalwani/Ibis_H3_Example">Ibis H3 Example</a></td>
    <td><ul>
          <li>This UDF shows how to open NYC yellow taxi trip dataset using Ibis with a DuckDB backend and aggregate the pickups using H3-DuckDB extension.</li>
        </ul>
    </td>
    <td> h3, duck-db, ibis </td>
    <td> Isaac Brodsky </td>

  </tr>

</table>

Explore more UDFs for your usecases or develop one using your own data source in the workbench

## 1. Join the community

- **Discord** - Our Discord community is excellent and we encourage you to share your progress on the **General** channel. We would love to hear what you are planning to build and help you throughout your journey.

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

Configure the settings in the workbench to increase your visibility and making it more convenient for others to reuse the UDF that you create.

- **Setting Default Parameters** - For UDFs that feature modifiable parameters to alter outputs, it's crucial to list all possible options under the "Default Parameter Values" section. This will ensure that these parameters are easily accessible via a dropdown menu in the UDF interface within the Layers section. Providing a comprehensive list of default parameters enhances usability and encourages experimentation.
- **Default View State** -Establish a Default View State for your UDF to set a predetermined viewport on the map. This feature ensures that users who load your UDF will start with a consistent view, offering a standardized initial experience. This setting is especially helpful for orienting new users to the UDF's intended geographic focus or area of interest.
- **Add Tags** - Tags play a vital role in increasing the discoverability of your UDF. When adding your UDF to the repository, include relevant tags that describe its functionality, application domain, and any other pertinent attributes. Well-chosen tags can significantly enhance the visibility of your UDF within the community, making it easier for users to find and utilize your work.
- **Description** - Describe your UDF! Elaborate on what your UDF accomplishes and mention your data sources and any relevant links. You can also mention the steps to run the UDF in jupyter notebook.This section supports Markdown links, code blocks, and headers are supported, which allows for organized presentation of your content.

## 5. Pre commit

Please run pre-commit hooks on your UDF prior to submitting.

```
pre-commit install
pre-commit run --files /*Your UDF Folder*/*
```

## 6. Submit a Pull request

- **Create a Branch** : To start, create a new branch specifically for your UDF. This helps isolate your changes and makes the review process more manageable. Use a descriptive name for your branch that reflects the nature of the UDF.
- **Commit Your Changes** : Once your UDF is ready and tested, commit your changes. Be sure to write a clear and descriptive commit message that explains what you've developed or changed.
- **Open a Pull Request** : Push your changes and open a Pull Request and let us get back to you for any reviews if needed.
- **View your latest UDF in the workbench Catalog!** : Once the Pull request is accepted and it is merged, you and everyone else will be able to view and import your unique UDF from within the workbench.

**Congratulations! You are an author of a UDF now which can be used and accessed by users around the globe 🌎**
