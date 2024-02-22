import requests
import pandas as pd
import geopandas as gpd
import pyarrow.parquet as pq
import shapely
import fused
def bbox_to_xy_slice(ds, bbox, offset=1):
    import numpy as np
    ny, nx=ds.lat.shape
#     ymin = ds.lat.min().item()
#     ymax = ds.lat.max().item()
    xmin = ds.lon.min().item()
    xmax = ds.lon.max().item()
    # print(ymin, ymax, xmin, xmax)
    xid0 = int(np.floor((bbox[0] - xmin) / (xmax - xmin) * (nx - 1)))
    xid1 = int(np.ceil((bbox[2] - xmin) / (xmax - xmin) * (nx - 1)))
    arr = ds.lat[:, 0].values
    yid0 = np.abs(arr-bbox[3]).argmin()
    yid1 = np.abs(arr-bbox[1]).argmin()
#     yid0 = ny-int(np.ceil((bbox[3] - ymin) / (ymax - ymin) * (ny - 1)))
#     yid1 = ny-int(np.floor((bbox[1] - ymin) / (ymax - ymin) * (ny - 1)))
    print(yid0,yid1)
    print(ny,nx)
    return slice(max(xid0-offset,0),xid1+offset), slice(max(yid0-offset,0),yid1+offset) 

def geom_to_centroid(dfx):
            dfx['lat'] = dfx.centroid.y
            dfx['lng'] = dfx.centroid.x
            return fused.utils.geo_convert(dfx[['lat','lng']])
    
def path_to_tile(x,y,z, path, path_meta, output='tile', 
                     col_list=['geometry','class','categories'], verbose=True):
    df_tile = xyz_to_gpd(x,y,z)
    output_options=['meta0','meta1','tile']
    if output not in output_options:
        raise Exception(f"output must by in {output_options}")
    import geopandas as gpd
    import shapely
    df = fused.utils.get_chunks_metadata(path_meta)
    df['fused_index']=0
    df2 = df.sjoin(df_tile)
    if output=='meta0':
        return df2
    a = run_async(lambda v: read_geoparquet_chunk(f'{path_meta}{v[0]}.parquet', v[1], col_list=None),   
              df2[['file_id','chunk_id']].values)
    df_chunks = pd.concat(a).sjoin(df_tile)[['file_idx','chunk_idx','geometry']]
    if verbose: print(f'{len(df_chunks)} chunks to fetch' )
    if output=='meta1':
        return df_chunks
    a = run_async(lambda v: read_geoparquet_chunk(f'{path}{v[0]}.parquet', v[1], col_list=col_list),   
              df_chunks[['file_idx','chunk_idx']].values[:])
    df_all = pd.concat(a)
    if verbose: print(f'{df_all.shape} rows before clip to tile',) 
    df_all = df_all.sjoin(df_tile)
    if verbose: print(f'{df_all.shape} rows after clip to tile',)
    del df_all['index_right']
    return df_all

def xyz_tile_to_bbox(y, x, zoom):
    import math
    n = 2.0 ** zoom
    lon1 = x / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
    lat1 = math.degrees(lat_rad)
    lon2 = (x + 1) / n * 360.0 - 180.0
    lat2_rad = math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n)))
    lat2 = math.degrees(lat2_rad)
    return (lon1, lat1, lon2, lat2)
def xyz_to_gpd(x,y,z):
    tile_bbox = shapely.box(*xyz_tile_to_bbox(y, x, z))
    return gpd.GeoDataFrame({}, geometry=[tile_bbox])

def read_geoparquet_chunk(file_path, chunk, col_list=None):
    parquet_file = pq.ParquetFile(file_path)
    if col_list:
        row_group = parquet_file.read_row_group(chunk, columns=col_list)
    else:
        row_group = parquet_file.read_row_group(chunk)
    data = row_group.to_pandas()
    if 'geometry' in data.columns:
        gdf = gpd.GeoDataFrame(data, geometry=shapely.from_wkb(data.geometry), crs=4326)
    else:
        gdf=data
    gdf['fused_index'] = range(len(gdf))
    return gdf

def read_metadata(path, context):
        r = requests.get("https://test-app.fusedlabs.io/server/v1/table/open",
                   params={"path":path},
                   headers={"Authorization": f"Bearer {context.auth_token}"}
                  )
        r.raise_for_status()
        table_metadata = r.json()
        chunk_metadata = table_metadata['chunk_metadata']
        df_meta = pd.DataFrame.from_dict(chunk_metadata)
        gdf_meta = gpd.GeoDataFrame(df_meta, geometry=df_meta.apply(lambda row:shapely.box(
                        row.bbox_minx,row.bbox_miny,row.bbox_maxx,row.bbox_maxy),1),crs=4326)
        return gdf_meta    
    
import asyncio
import asyncio.events as events
import os
import sys
import threading
from contextlib import contextmanager, suppress
from heapq import heappop

    
"""Patch asyncio to allow nested event loops."""

import asyncio
import asyncio.events as events
import os
import sys
import threading
from contextlib import contextmanager, suppress
from heapq import heappop


def apply(loop=None):
    """Patch asyncio to make its event loop reentrant."""
    _patch_asyncio()
    _patch_policy()
    _patch_task()
    _patch_tornado()

    loop = loop or asyncio.get_event_loop()
    _patch_loop(loop)


def _patch_asyncio():
    """Patch asyncio module to use pure Python tasks and futures."""

    def run(main, *, debug=False):
        loop = asyncio.get_event_loop()
        loop.set_debug(debug)
        task = asyncio.ensure_future(main)
        try:
            return loop.run_until_complete(task)
        finally:
            if not task.done():
                task.cancel()
                with suppress(asyncio.CancelledError):
                    loop.run_until_complete(task)

    def _get_event_loop(stacklevel=3):
        loop = events._get_running_loop()
        if loop is None:
            loop = events.get_event_loop_policy().get_event_loop()
        return loop

    # Use module level _current_tasks, all_tasks and patch run method.
    if hasattr(asyncio, '_nest_patched'):
        return
    if sys.version_info >= (3, 6, 0):
        asyncio.Task = asyncio.tasks._CTask = asyncio.tasks.Task = \
            asyncio.tasks._PyTask
        asyncio.Future = asyncio.futures._CFuture = asyncio.futures.Future = \
            asyncio.futures._PyFuture
    if sys.version_info < (3, 7, 0):
        asyncio.tasks._current_tasks = asyncio.tasks.Task._current_tasks
        asyncio.all_tasks = asyncio.tasks.Task.all_tasks
    if sys.version_info >= (3, 9, 0):
        events._get_event_loop = events.get_event_loop = \
            asyncio.get_event_loop = _get_event_loop
    asyncio.run = run
    asyncio._nest_patched = True


def _patch_policy():
    """Patch the policy to always return a patched loop."""

    def get_event_loop(self):
        if self._local._loop is None:
            loop = self.new_event_loop()
            _patch_loop(loop)
            self.set_event_loop(loop)
        return self._local._loop

    policy = events.get_event_loop_policy()
    policy.__class__.get_event_loop = get_event_loop


def _patch_loop(loop):
    """Patch loop to make it reentrant."""

    def run_forever(self):
        with manage_run(self), manage_asyncgens(self):
            while True:
                self._run_once()
                if self._stopping:
                    break
        self._stopping = False

    def run_until_complete(self, future):
        with manage_run(self):
            f = asyncio.ensure_future(future, loop=self)
            if f is not future:
                f._log_destroy_pending = False
            while not f.done():
                self._run_once()
                if self._stopping:
                    break
            if not f.done():
                raise RuntimeError(
                    'Event loop stopped before Future completed.')
            return f.result()

    def _run_once(self):
        """
        Simplified re-implementation of asyncio's _run_once that
        runs handles as they become ready.
        """
        ready = self._ready
        scheduled = self._scheduled
        while scheduled and scheduled[0]._cancelled:
            heappop(scheduled)

        timeout = (
            0 if ready or self._stopping
            else min(max(
                scheduled[0]._when - self.time(), 0), 86400) if scheduled
            else None)
        event_list = self._selector.select(timeout)
        self._process_events(event_list)

        end_time = self.time() + self._clock_resolution
        while scheduled and scheduled[0]._when < end_time:
            handle = heappop(scheduled)
            ready.append(handle)

        for _ in range(len(ready)):
            if not ready:
                break
            handle = ready.popleft()
            if not handle._cancelled:
                handle._run()
        handle = None

    @contextmanager
    def manage_run(self):
        """Set up the loop for running."""
        self._check_closed()
        old_thread_id = self._thread_id
        old_running_loop = events._get_running_loop()
        try:
            self._thread_id = threading.get_ident()
            events._set_running_loop(self)
            self._num_runs_pending += 1
            if self._is_proactorloop:
                if self._self_reading_future is None:
                    self.call_soon(self._loop_self_reading)
            yield
        finally:
            self._thread_id = old_thread_id
            events._set_running_loop(old_running_loop)
            self._num_runs_pending -= 1
            if self._is_proactorloop:
                if (self._num_runs_pending == 0
                        and self._self_reading_future is not None):
                    ov = self._self_reading_future._ov
                    self._self_reading_future.cancel()
                    if ov is not None:
                        self._proactor._unregister(ov)
                    self._self_reading_future = None

    @contextmanager
    def manage_asyncgens(self):
        if not hasattr(sys, 'get_asyncgen_hooks'):
            # Python version is too old.
            return
        old_agen_hooks = sys.get_asyncgen_hooks()
        try:
            self._set_coroutine_origin_tracking(self._debug)
            if self._asyncgens is not None:
                sys.set_asyncgen_hooks(
                    firstiter=self._asyncgen_firstiter_hook,
                    finalizer=self._asyncgen_finalizer_hook)
            yield
        finally:
            self._set_coroutine_origin_tracking(False)
            if self._asyncgens is not None:
                sys.set_asyncgen_hooks(*old_agen_hooks)

    def _check_running(self):
        """Do not throw exception if loop is already running."""
        pass

    if hasattr(loop, '_nest_patched'):
        return
    if not isinstance(loop, asyncio.BaseEventLoop):
        raise ValueError('Can\'t patch loop of type %s' % type(loop))
    cls = loop.__class__
    cls.run_forever = run_forever
    cls.run_until_complete = run_until_complete
    cls._run_once = _run_once
    cls._check_running = _check_running
    cls._check_runnung = _check_running  # typo in Python 3.7 source
    cls._num_runs_pending = 0
    cls._is_proactorloop = (
        os.name == 'nt' and issubclass(cls, asyncio.ProactorEventLoop))
    if sys.version_info < (3, 7, 0):
        cls._set_coroutine_origin_tracking = cls._set_coroutine_wrapper
    cls._nest_patched = True


def _patch_task():
    """Patch the Task's step and enter/leave methods to make it reentrant."""

    def step(task, exc=None):
        curr_task = curr_tasks.get(task._loop)
        try:
            step_orig(task, exc)
        finally:
            if curr_task is None:
                curr_tasks.pop(task._loop, None)
            else:
                curr_tasks[task._loop] = curr_task

    Task = asyncio.Task
    if hasattr(Task, '_nest_patched'):
        return
    if sys.version_info >= (3, 7, 0):

        def enter_task(loop, task):
            curr_tasks[loop] = task

        def leave_task(loop, task):
            curr_tasks.pop(loop, None)

        asyncio.tasks._enter_task = enter_task
        asyncio.tasks._leave_task = leave_task
        curr_tasks = asyncio.tasks._current_tasks
        step_orig = Task._Task__step
        Task._Task__step = step
    else:
        curr_tasks = Task._current_tasks
        step_orig = Task._step
        Task._step = step
    Task._nest_patched = True


def _patch_tornado():
    """
    If tornado is imported before nest_asyncio, make tornado aware of
    the pure-Python asyncio Future.
    """
    if 'tornado' in sys.modules:
        import tornado.concurrent as tc  # type: ignore
        tc.Future = asyncio.Future
        if asyncio.Future not in tc.FUTURES:
            tc.FUTURES += (asyncio.Future,)
            
            
            
def run_async(fn, arr_args, delay=0, max_workers=32):
    apply()
    import numpy as np
    import concurrent.futures
    loop = asyncio.get_event_loop()
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    async def fn_async(pool, fn, *args):
        try:
            
                result = await loop.run_in_executor(pool, fn, *args)
                return result
        except OSError as error:
            print(f"Error: {error}")
            return None
   
    async def fn_async_exec(fn, arr, delay):
        tasks = []
        await asyncio.sleep(delay*np.random.random())
        if type(arr[0])==list or type(arr[0])==tuple:
            pass
        else:
            arr = [[i] for i in arr]
        for i in arr:
            tasks.append(fn_async(pool,fn,*i))
        return await asyncio.gather(*tasks)

    return loop.run_until_complete(fn_async_exec(fn, arr_args, delay))