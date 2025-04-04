@fused.udf
def nail_udf(i=1):
    return i + 1 * 150


submit_list = [{"i": i} for i in range(4)]


##============================= Batch Runner ======================================
@fused.udf(cache_max_age="0s")
def udf(
    nail_udf=nail_udf,
    df_arg: list = submit_list,
    instance_type: str = "t3.small",
    disk_size_gb: int = 100,
    return_html: bool = True,  # to return html or job_id
    debug: bool = True,  # to run for only one arg
):
    import pandas as pd

    if isinstance(df_arg, list):  # todo: eval more types & add error handling
        df_arg = pd.DataFrame(df_arg)
    if debug:
        udf = fused.models.udf.udf.GeoPandasUdfV2.parse_raw(common.udf_to_json(nail_udf))
        return fused.run(udf, **df_arg.to_dict(orient="records")[0], engine="remote")
    else:
        job_url = submit_job_once(nail_udf, df_arg, instance_type, disk_size_gb)
        if return_html:
            return common.url_redirect(job_url)
        else:
            return job_url


@fused.cache(cache_max_age="12h")
def submit_job_once(nail_udf, df_arg, instance_type="t3.small", disk_size_gb=100, verbose=False):
    job = common.submit_job(nail_udf, df_arg, cache_max_age="0s")
    j = job.run_remote(instance_type=instance_type, disk_size_gb=disk_size_gb)
    job_url = fused.options.base_url.replace("server/v1", f"job_status/{j.job_id}")
    if verbose:
        print(job_url)
        print(job_url)
    return job_url


common = fused.load("https://github.com/fusedio/udfs/tree/c39f643/public/common/").utils
