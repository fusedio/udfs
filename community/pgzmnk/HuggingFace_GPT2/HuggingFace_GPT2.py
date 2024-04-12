@fused.udf
def udf(input_text = "Earth observation is..."):
    import os
    import pandas as pd
    from transformers import pipeline, set_seed

    # Set HF and Torch cache directories to Fused filesystem
    os.environ['HF_HOME'] = os.environ['HF_HUB_CACHE'] = '/mnt/cache/'
    os.environ['TORCH_WHERE'] = os.environ['TORCH_HOME'] = '/mnt/cache/tmpa/tmp/'

    # Cache generator pipeline object so it only downloads model once
    @fused.cache(reset=True)
    def load_generator():
        return pipeline('text-generation', model='gpt2-medium')
    
    generator = load_generator()

    # Seed for reproduceability during testing
    set_seed(42)

    # Run model directly with a text generation pipeline
    out = generator(
        input_text, 
        max_length=60, 
        num_return_sequences=2,
        pad_token_id=50256
    )

    # Preview responses
    for each in out:
        print(each)

    # Return structured data
    return pd.DataFrame({
        'input_text': [input_text],
        'output_text': [out[-1]['generated_text']]
    })
    
