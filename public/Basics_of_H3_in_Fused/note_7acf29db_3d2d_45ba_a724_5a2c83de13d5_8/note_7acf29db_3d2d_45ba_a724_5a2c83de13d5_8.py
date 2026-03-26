# 💡 **Start small with** **parallel jobs**

Before running all 60 months in `era5_full_year`, test with 2-3 months first. Bugs are cheaper to catch on 3 jobs than 60, and you avoid wasting compute on a broken UDF.

```python
months[:3]  # Test on Jan–Mar before running all 60
pool = monthly_udf.map([{'month': m, 'bounds': list(bounds)} for m in months[:3]])
```