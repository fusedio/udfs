@fused.udf
def udf(
    bounds: fused.types.Bounds = None,
    wms_url: str = "https://wms.geo.admin.ch/",
    layer: str = "ch.swisstopo.pixelkarte-farbe",
    width: int = 256,
    height: int = 256,
):
    import numpy as np
    import utils
    from utils import fetch_wms

    z = utils.common.estimate_zoom(bounds)
    print(f"Estimated zoom level: {z}")

    if z < 4:
        print(
            "WARNING: Please zoom in more for better visualization. Zoom level should be at least 4."
        )

        return np.zeros((4, height, width), dtype=np.uint8)

    data = fetch_wms(
        wms_url=wms_url, layer=layer, bbox_coords=bounds, width=width, height=height
    )

    return data
