"""Download client (sentinelhub-py download/client.py): runs requests in this process, or in parallel on Fused workers.

Through `.map` an xarray Dataset or raw bytes come back as a GeoTIFF, and numpy fails. So the worker sends a
one-row DataFrame (kind, payload bytes, meta JSON) made by `to_payload`, and `download` rebuilds the result with
`from_payload`: the same object that `get_data` gives.
The worker is a UDF made here from a small entry and the source of each module it needs (as headers), so a worker
loads no module by name. This works in the canvas, from a package folder or a GitHub URL, and inside a remote UDF.
"""

try:
    import su_constants as constants
    import su_process as process
    import su_statistical as statistical
except ImportError:
    constants = fused.load("su_constants")
    process = fused.load("su_process")
    statistical = fused.load("su_statistical")

ENGINES = ("remote", "local")
REQUEST_TYPES = ("process", "statistical")
SAVE_DIR = "fd://sentinel_utils/data"
REMOTE_TIMEOUT = 120
POLL_SECONDS = 0.05
FINAL_ERRORS = ("NoDataError", "ValueError")
WORKER_CODE = '''@fused.udf(cache_max_age="0s")
def udf(request: str = ""):
    import json
    import su_download

    return su_download.to_payload(su_download.get_data(json.loads(request)))
'''


@fused.udf
def udf():
    return api()


def api():
    """Returns a DataFrame of the public functions: name, signature, doc."""
    return constants.api_table(globals())


def get_data(request):
    """Returns the result of one request (from process_request or statistical_request), run in this process."""
    request = check_request(request)
    if request["type"] == "process":
        return process.run_process(request)
    return statistical.run_statistical(request)


def check_request(request):
    """Returns the request after it validates it again through its constructor; other dicts raise ValueError."""
    if not isinstance(request, dict):
        raise ValueError(f"a request must be a dict made by process_request or statistical_request, "
                         f"got {type(request).__name__}")
    constants.check_choice("request type", request.get("type"), REQUEST_TYPES)
    if request["type"] == "process":
        return process.check_request(request)
    return statistical.check_request(request)


def download(requests, *, max_workers=16, engine="remote", ignore_errors=False):
    """Returns the results in request order; engine remote runs each on a Fused worker (worker()), local in threads."""
    import warnings

    outcomes = _collect(requests, max_workers, engine)
    failed = [(i, err) for i, (_, err) in enumerate(outcomes) if err is not None]
    if failed:
        msg = _failures(failed, len(outcomes))
        if not ignore_errors:
            raise RuntimeError(msg)
        warnings.warn(msg)
    return [result for result, _ in outcomes]


def iter_download(requests, *, max_workers=16, engine="remote", ignore_errors=False):
    """Yields (index, result) in the order the requests finish; index is the position of the request in requests."""
    checked, workers = _prepare(requests, max_workers, engine)
    return _progressive(checked, workers, engine, ignore_errors)


def save_data(request, path=None):
    """Returns the path of the saved result: a COG (PNG for output png) or Parquet; default folder SAVE_DIR."""
    import hashlib
    import json
    import os
    import shutil
    import tempfile

    request = check_request(request)
    if request["type"] == "process":
        ext = "png" if request["output"] == "png" else "tif"
        request = check_request({**request, "output": "png" if ext == "png" else "geotiff"})
    else:
        ext = "parquet"
    key = hashlib.sha1(json.dumps(request, sort_keys=True).encode()).hexdigest()[:12]
    path = str(path or f"{SAVE_DIR}/{request['type']}_{key}.{ext}")
    data = get_data(request)
    local = os.path.join(tempfile.mkdtemp(), os.path.basename(path))
    if ext == "parquet":
        data.to_parquet(local)
    else:
        with open(local, "wb") as f:
            f.write(data)
    if path.startswith(("fd://", "s3://")):
        fused.api.upload(local, path)
    else:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        shutil.copy(local, path)
    return path


def worker():
    """Returns the worker Udf: WORKER_CODE plus the source of each module it needs as a header (no load by name)."""
    import linecache

    modules = {"su_constants": constants, "su_geometry": process.geometry, "su_time": process.time,
               "su_catalog": process.catalog, "su_reader": process.reader, "su_indices": process.indices,
               "su_embeddings": process.embeddings, "su_process": process, "su_statistical": statistical,
               "su_download": globals()}
    headers = []
    for name, module in modules.items():
        loader = module.get("__loader__") if isinstance(module, dict) else getattr(module, "__loader__", None)
        source = (getattr(module, "code", None) or getattr(loader, "source_code", None)
                  or "".join(linecache.getlines(f"<module {name}>")))
        if not source:
            raise RuntimeError(f"no source found for {name}; load sentinel_utils with fused.load first")
        headers.append({"module_name": name, "source_code": source})
    return fused.models.Udf.model_validate(
        {"name": "su_worker", "type": "geopandas_v2", "entrypoint": "udf", "code": WORKER_CODE, "headers": headers,
         "metadata": {}}, context={"import_globals": False, "load_parameter_list": True})


def to_payload(result):
    """Returns a one-row DataFrame (kind, payload, meta) that carries a result of get_data through `.map` unchanged."""
    import io
    import json
    import numpy as np
    import pandas as pd
    import xarray as xr

    meta = {}
    if isinstance(result, xr.Dataset):
        ds = result.drop_vars("spatial_ref", errors="ignore")
        names = [*ds.data_vars, *ds.coords]
        meta = {"layers": [[k, list(ds[k].dims), dict(ds[k].attrs), k in ds.coords] for k in names],
                "attrs": dict(result.attrs), "crs": str(_odc(result).crs) if _odc(result).crs is not None else None}
        kind, data = "dataset", _npz([ds[k].values for k in names])
    elif isinstance(result, np.ndarray):
        kind, data = "array", _npz([result])
    elif isinstance(result, (bytes, bytearray)):
        kind, data = "bytes", bytes(result)
    elif isinstance(result, pd.DataFrame):
        buf = io.BytesIO()
        result.to_parquet(buf)
        kind, data, meta = "frame", buf.getvalue(), {"geo": hasattr(result, "geometry") and "geometry" in result}
    else:
        kind, data = "json", json.dumps(result).encode()
    return pd.DataFrame({"kind": [kind], "payload": [data], "meta": [json.dumps(meta, default=_plain)]})


def from_payload(frame):
    """Returns the result that to_payload packed: xarray Dataset, numpy array, bytes, DataFrame or JSON value."""
    import io
    import json
    import numpy as np
    import pandas as pd

    kind, data, meta = frame["kind"].iloc[0], frame["payload"].iloc[0], json.loads(frame["meta"].iloc[0])
    if kind == "dataset":
        import xarray as xr

        with np.load(io.BytesIO(data), allow_pickle=False) as z:
            arrays = [z[f"arr_{i}"] for i in range(len(z.files))]
        layers = [(name, xr.Variable(dims, arr, attrs), coord)
                  for (name, dims, attrs, coord), arr in zip(meta["layers"], arrays)]
        ds = xr.Dataset({n: v for n, v, coord in layers if not coord}, coords={n: v for n, v, coord in layers if coord},
                        attrs=meta["attrs"])
        return _odc(ds).assign_crs(meta["crs"]) if meta["crs"] else ds
    if kind == "array":
        with np.load(io.BytesIO(data), allow_pickle=False) as z:
            return z["arr_0"]
    if kind == "bytes":
        return bytes(data)
    if kind == "frame":
        if meta.get("geo"):
            import geopandas as gpd

            return gpd.read_parquet(io.BytesIO(data))
        return pd.read_parquet(io.BytesIO(data))
    return json.loads(data)


def _prepare(requests, max_workers, engine):
    constants.check_choice("engine", engine, ENGINES)
    if isinstance(requests, dict):
        raise ValueError("requests must be a list of request dicts; put one request in a list")
    checked = [check_request(r) for r in requests]
    return checked, max(1, min(int(max_workers), len(checked) or 1))


def _collect(requests, max_workers, engine):
    checked, workers = _prepare(requests, max_workers, engine)
    outcomes = [(None, None)] * len(checked)
    stream = _stream(checked, workers, engine)
    try:
        for index, result, err in stream:
            outcomes[index] = (result, err)
    finally:
        stream.close()
    return outcomes


def _no_data(err):
    return isinstance(err, constants.NoDataError) or "NoDataError" in str(err)


def _progressive(requests, workers, engine, ignore_errors):
    import warnings

    failed = []
    stream = _stream(requests, workers, engine)
    try:
        for index, result, err in stream:
            if err is not None:
                if not ignore_errors:
                    raise RuntimeError(f"request {index} of {len(requests)} failed: {_message(err)}") from err
                failed.append((index, err))
            yield index, result
    finally:
        stream.close()
    if failed:
        warnings.warn(_failures(sorted(failed, key=lambda f: f[0]), len(requests)))


def _stream(requests, workers, engine):
    if requests:
        yield from _iter_remote(requests, workers) if engine == "remote" else _iter_local(requests, workers)


def _iter_local(requests, workers):
    from concurrent.futures import ThreadPoolExecutor, as_completed

    executor = ThreadPoolExecutor(workers)
    try:
        futures = {executor.submit(get_data, r): i for i, r in enumerate(requests)}
        for future in as_completed(futures):
            err = future.exception()
            yield futures[future], None if err else future.result(), err
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


def _iter_remote(requests, workers):
    import asyncio
    import numbers

    timeout = fused.options.request_timeout
    if isinstance(timeout, numbers.Real) and timeout < REMOTE_TIMEOUT:
        fused.options.request_timeout = REMOTE_TIMEOUT
    pools = []
    try:
        pools.append(_submit(requests, list(range(len(requests))), workers))
        left, retried = set(range(len(requests))), set()
        while left:
            again = []
            for pool, indices, seen in pools:
                for j, got in sorted(pool.results_now(return_exceptions=True).items()):
                    index = indices[j]
                    if j in seen or index not in left:
                        continue
                    seen.add(j)
                    if isinstance(got, BaseException) and index not in retried and _retryable(got):
                        retried.add(index)
                        again.append(index)
                        continue
                    left.discard(index)
                    yield _landed(index, got)
            if again:
                pools.append(_submit(requests, again, workers))
            if left:
                asyncio.run(asyncio.sleep(POLL_SECONDS))
    finally:
        for pool, _, _ in pools:
            pool.cancel(wait=True)
        fused.options.request_timeout = timeout


def _submit(requests, indices, workers):
    import json

    args = [{"request": json.dumps(requests[i])} for i in indices]
    pool = worker().map(args, max_workers=min(workers, len(args)), max_retry=0, cache_max_age="0s")
    return pool, indices, set()


def _retryable(err):
    return not any(name in str(err) for name in FINAL_ERRORS)


def _landed(index, got):
    if isinstance(got, BaseException):
        return index, None, got
    try:
        return index, from_payload(got), None
    except Exception as e:
        return index, None, e


def _failures(failed, n):
    first, err = failed[0]
    return f"{len(failed)} of {n} requests failed; first: request {first}: {_message(err)}"


def _message(err):
    lines = [s.strip() for s in str(err).splitlines() if s.strip()]
    return f"{type(err).__name__}: {lines[-1] if lines else repr(err)}"[:300]


def _npz(arrays):
    import io
    import numpy as np

    buf = io.BytesIO()
    np.savez_compressed(buf, *[a.astype(str) if a.dtype == object else a for a in arrays])
    return buf.getvalue()


def _plain(value):
    return value.item() if hasattr(value, "item") else str(value)


def _odc(obj):
    import odc.geo.xr

    return obj.odc
