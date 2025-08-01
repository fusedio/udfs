common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
@fused.udf
def udf_nail(i=1):
    return i + 1 * 150


submit_list = [{"i": i} for i in range(128)]

udf_nail_json = common.udf_to_json(udf_nail)
##============================= Batch Runner ======================================
@fused.udf(cache_max_age="0s")
def udf(
    udf_nail_json=udf_nail_json,
    submit_list: list = submit_list,
    instance_type: str = "t3.small",
    disk_size_gb: int = 100,
    return_html: bool = True,  # to return html or job_id
    debug: bool = True,  # to run for only one arg
):
    
    if isinstance(submit_list, list):  # todo: eval more types & add error handling
        import pandas as pd
        df_args = pd.DataFrame(submit_list)
    else:
        df_args = submit_list
    if debug:
        udf_nail = fused.models.udf.udf.GeoPandasUdfV2.parse_raw(udf_nail_json)
        return fused.run(udf_nail, **df_args.to_dict(orient="records")[0], engine="remote")
    else:
        job_url = submit_job_once(udf_nail_json, df_args, instance_type, disk_size_gb)
        if return_html:
            return common.url_redirect(job_url)
        else:
            return job_url


@fused.cache(cache_max_age="12h")
def submit_job_once(udf_nail_json, submit_list, instance_type="t3.small", disk_size_gb=100, verbose=False):
    udf_nail = fused.models.udf.udf.GeoPandasUdfV2.parse_raw(udf_nail_json)
    job = common.submit_job(udf_nail, submit_list, cache_max_age="0s")
    j = job.run_remote(instance_type=instance_type, disk_size_gb=disk_size_gb)
    job_url = fused.options.base_url.replace("server/v1", f"job_status/{j.job_id}")
    if verbose:
        print(job_url)
        print(job_url)
    return job_url
