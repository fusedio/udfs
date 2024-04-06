import fused
import gzip

def load_h3_duckdb(con, *, cache_ungzipped_path = True):
    import duckdb

    con.sql("SET home_directory='/tmp';")
    url = f'https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev/v{duckdb.__version__}/linux_amd64_gcc4/h3ext.duckdb_extension.gz'
    # Note this is not the correct file name, it will be fixed later in this function
    # This workaround of downloading in Python is needed because DUckDB cannot load extensions
    # from https (ssl, secure) URLs.
    file_name = 'h3ext_gcc4.duckdb_extension.gz'
    downloaded_path = fused.download(url, file_name)

    def ungzip_ext(url, from_path, to_path):
        with gzip.open(from_path, 'rb') as fin:
            with open(to_path, 'wb') as fout:
                buf = fin.read()
                fout.write(buf)
        print('ungzipped H3 extension', url, from_path, to_path)

    # The name of this file MUST be h3ext.duckdb_extension, or else DuckDB will not install it in the right place
    ungzip_path = '/mnt/cache/h3ext.duckdb_extension'
    ungzip_ext_fn = fused.cache(ungzip_ext) if cache_ungzipped_path else ungzip_ext
    ungzip_ext_fn(url, downloaded_path, ungzip_path)
    
    con.sql(f"""INSTALL '{ungzip_path}';
    LOAD h3ext;""")
