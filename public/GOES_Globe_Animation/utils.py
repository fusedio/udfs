def file_of_day(i, bucket_name, product_name, band, start_datestr='2024-01-30', hour_offset='7'):
    if product_name in ('ABI-L1b-RadF', 'ABI-L2-CMIPF'):
        nfile=6
        prename=f'M6C{band:02.0f}'
    elif product_name in ('ABI-L1b-RadC', 'ABI-L2-CMIPC'):
        nfile=12
        prename=f'M6C{band:02.0f}'
    elif product_name in ('ABI-L2-RRQPEF'):
        nfile=6
        prename=f'M6_G' 
    else:
        print(f'{product_name} not in ABI-L1b-RadF | ABI-L1b-RadC | ABI-L2-CMIPF')

    import datetime
    import numpy as np
    import s3fs
    start_time = int(datetime.datetime.strptime(start_datestr, '%Y-%m-%d').timestamp())
    hourly_timestamps = []
    hour = i//nfile
    hour_timestamp = start_time + hour * 3600  
    start_datetime = datetime.datetime.strptime(start_datestr, '%Y-%m-%d')
    hour_datetime = start_datetime + datetime.timedelta(hours=hour+int(hour_offset))
    day_of_year = hour_datetime.timetuple().tm_yday
    hourly_timestamps.append([hour_datetime.year, day_of_year, hour_datetime.hour])
    blob = f'{product_name}/{hour_datetime.year}/{day_of_year:03.0f}/{hour_datetime.hour:02.0f}'
    prefix = f'OR_{product_name}-{prename}'
    s3 = s3fs.S3FileSystem(anon=True)
    flist = s3.ls(f'{bucket_name}/{blob}')
    flist = np.sort([i for i in flist if i.split('/')[-1].startswith(prefix)])
    return flist[i%nfile][len(bucket_name)+1:]

