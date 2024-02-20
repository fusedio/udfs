@fused.udf
def udf(*args, **kwargs):
    from utils import my_util, the_udf
    my_util()
    return the_udf(*args, **kwargs)