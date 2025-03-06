import os

import fused

# Authenticate
if os.environ.get("CI"):
    fused._auth.refresh_token(os.getenv("AUTH0_REFRESH_TOKEN"))
