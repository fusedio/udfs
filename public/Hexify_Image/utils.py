def image_to_bound(image):
    shape = image.shape
    w, h = (shape[-1], shape[-2])
    if w > h:
        return (0, 0, 1, 1 / (w / h))
    else:
        return (0, 0, 1 / (h / w), 1)