import fused
import gzip

def load_h3_duckdb(con, *, cache_ungzipped_path = True):
    import duckdb
    import platform

    con.sql("SET home_directory='/tmp';")
    system = platform.system()
    arch = platform.platform()
    arch = 'amd64' if arch == 'x86_64' else arch

    detected_os = f'macos_{arch}' if system == 'Darwin' else f'linux_{arch}'
    if detected_os == 'linux_amd64':
        detected_os = 'linux_amd64_gcc4'
    url = f'https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev/v{duckdb.__version__}/{detected_os}/h3ext.duckdb_extension.gz'
    # Note this is not the correct file name, it will be fixed later in this function
    # This workaround of downloading in Python is needed because DuckDB cannot load extensions
    # from https (ssl, secure) URLs.
    file_name = 'h3ext_downloaded.duckdb_extension.gz'
    downloaded_path = fused.download(url, file_name)

    def ungzip_ext(url, from_path, to_path):
        with gzip.open(from_path, 'rb') as fin:
            with open(to_path, 'wb') as fout:
                buf = fin.read()
                fout.write(buf)
        print('ungzipped H3 extension', url, from_path, to_path)

    # The name of this file MUST be h3ext.duckdb_extension, or else DuckDB will not install it in the right place
    ungzip_path = fused.core.create_path('h3ext.duckdb_extension')
    ungzip_ext_fn = fused.cache(ungzip_ext) if cache_ungzipped_path else ungzip_ext
    ungzip_ext_fn(url, downloaded_path, ungzip_path)
    
    con.sql(f"""INSTALL '{ungzip_path}';
    LOAD h3ext;""")
