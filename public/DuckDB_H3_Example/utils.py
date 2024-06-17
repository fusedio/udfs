import fused


def duckdb_with_h3(extra_config=None, extra_connect_args=None):
    import duckdb

    con = duckdb.connect(
        config={
            "allow_unsigned_extensions": True,
            **(extra_config if extra_config is not None else {}),
        },
        **(extra_connect_args if extra_connect_args is not None else {}),
    )
    load_h3_duckdb(con)
    return con


def load_h3_duckdb(con):
    import duckdb

    new_home_path = fused.core.create_path(f"duckdb/{duckdb.__version__}/")
    con.sql(f"SET home_directory='{new_home_path}';")
    con.sql("INSTALL h3ext from 'https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev';")
    con.sql("LOAD h3ext;")
