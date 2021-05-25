

def is_shub(img):
    return img.startswith("shub://")

def is_sylabs(img):
    return img.startswith("library://")

def is_singularity(image_spec):
    # sylabs container registry or (deprecated) singlarity hub
    return is_shub(image_spec) or is_sylabs(image_spec)

def get_singularity_container_size(image_spec):
    # get image size - worst case scenario just download it
    return None