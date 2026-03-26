## **1.** Rolling up

`h3_cell_to_parent` takes any hex at a fine resolution and find its parent at a coarser one:

```
h3_cell_to_parent(hex, 4)
```

This maps every res‑15 hex up to the res‑4 hex that contains it. All the small hexes that share the same parent get grouped together.