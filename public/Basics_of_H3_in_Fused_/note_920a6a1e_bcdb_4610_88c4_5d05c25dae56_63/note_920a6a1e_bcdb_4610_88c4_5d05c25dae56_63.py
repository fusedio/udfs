## **2. Choosing an aggregation method**

When rolling up, multiple fine hexes collapse into one coarse hex so we need to decide *how* to combine their values:

- **Average**: use when values are *measurements* (e.g. temperature).
- **Sum**: use when values are *counts* (e.g. number of noise complaints in part A).

Since our data is **temperature**, we use `AVG()` and round it to 2 digits