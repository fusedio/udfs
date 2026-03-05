@fused.udf(cache_max_age=0)
def udf(
    token: str = "Bearer token",
    allowed_issuer_hosts: list = [
        "auth0.com",
        "okta.com",
        "oktapreview.com",
        "okta-emea.com",
        "login.microsoftonline.com",
        "sts.windows.net",
        "accounts.google.com",
    ],
):
    import jwt
    import httpx
    from urllib.parse import urlparse

    # extract token - expects "Bearer xxxx" format
    parts = token.split()
    if len(parts) != 2 or parts[0] != "Bearer":
        raise Exception("token must be in 'Bearer <token>' format")
    token = parts[1]

    # decode without verifying
    header = jwt.get_unverified_header(token)
    payload = jwt.decode(token, options={"verify_signature": False})

    issuer = payload["iss"]

    # validation
    parsed = urlparse(issuer)
    if parsed.scheme != "https":
        raise Exception("issuer must use https")
    
    host = parsed.hostname   
    if host is None or not any(host.endswith(x) for x in allowed_issuer_hosts):
        raise Exception(f"issuer host not allowed: {host}")

    # fetch OIDC discovery
    discovery_url = issuer.rstrip("/") + "/.well-known/openid-configuration"
    discovery = httpx.get(discovery_url, timeout=5).json()

    jwks_uri = discovery["jwks_uri"]

    # fetch JWKS
    jwks = httpx.get(jwks_uri, timeout=5).json()

    # locate signing key via kid
    kid = header["kid"]

    key = None
    for k in jwks["keys"]:
        if k["kid"] == kid:
            key = jwt.algorithms.RSAAlgorithm.from_jwk(k)
            break

    if key is None:
        raise Exception("signing key not found")

    # verify token
    decoded = jwt.decode(
        token,
        key,
        algorithms=["RS256"],
        audience=payload.get("aud"),
        issuer=issuer,
    )

    return decoded
