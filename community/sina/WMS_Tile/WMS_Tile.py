@fused.udf
def udf(
    bounds: fused.types.Bounds = [6.581,45.869,10.164,47.573],
    wms_url: str = "https://wms.geo.admin.ch/",
    layer: str = "ch.swisstopo.pixelkarte-farbe",
    width: int = 256,
    height: int = 256,
):
    import numpy as np
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")

    z = common.estimate_zoom(bounds)
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


def fetch_wms(
    wms_url: str,
    layer: str,
    bbox_coords: tuple,
    width: int,
    height: int,
    version: str = "1.3.0",
    format: str = "image/png",
    crs: str = "EPSG:4326",
    transparent: bool = True,
):
    from io import BytesIO
    import numpy as np
    import requests
    from PIL import Image

    try:
        minx, miny, maxx, maxy = bbox_coords

        # Handle bbox order based on WMS version and CRS
        if version == "1.3.0" and crs.upper() in ["EPSG:4326", "CRS:84"]:
            bbox_str = f"{miny},{minx},{maxy},{maxx}"
        else:
            bbox_str = f"{minx},{miny},{maxx},{maxy}"

        # Construct WMS parameters
        params = {
            "SERVICE": "WMS",
            "VERSION": version,
            "REQUEST": "GetMap",
            "LAYERS": layer,
            "STYLES": "",
            "CRS" if version == "1.3.0" else "SRS": crs,
            "BBOX": bbox_str,
            "WIDTH": width,
            "HEIGHT": height,
            "FORMAT": format,
            "TRANSPARENT": "TRUE" if transparent else "FALSE",
        }

        response = requests.get(wms_url, params=params)

        if response.status_code != 200:
            print(f"Error: HTTP status {response.status_code}")

        # Check content type
        content_type = response.headers.get("Content-Type", "")

        # Process image response
        if "image" in content_type.lower() or response.content[:4] in [
            b"\xff\xd8\xff\xe0",
            b"\x89PNG",
        ]:
            img = Image.open(BytesIO(response.content))
            array = np.array(img)

            # Convert to channels-first format
            if len(array.shape) == 3:  # RGB or RGBA
                array = array.transpose(2, 0, 1)
            else:  # Grayscale
                array = array[np.newaxis, :, :]

            return array
        else:
            print(f"Error: Response not an image. Content type: {content_type}")

    except Exception as e:
        print(f"Error fetching WMS: {e}")

