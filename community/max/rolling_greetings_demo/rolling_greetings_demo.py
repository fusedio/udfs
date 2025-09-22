@fused.udf
def udf(name: str = "world"):
    import pandas as pd
    import random

    greetings = ["hello", "hi", "hey", "greetings", "salutations"]
    selected_greeting = random.choice(greetings)

    return pd.DataFrame({selected_greeting: [name]})