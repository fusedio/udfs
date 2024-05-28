import ee

def ee_initialize(service_account_name="", key_path=""):
    """
    Authenticate with Google Earth Engine using service account credentials.

    This function initializes the Earth Engine API by authenticating with the 
    provided service account credentials. The authenticated session allows for 
    accessing and manipulating data within Google Earth Engine.

    Args:
        service_account_name (str): The email address of the service account.
        key_path (str): The path to the private key file for the service account.

    See documentation: https://docs.fused.io/basics/in/gee/

    Example:
        ee_initialize('your-service-account@your-project.iam.gserviceaccount.com', 'path/to/your-private-key.json')
    """
    credentials = ee.ServiceAccountCredentials(service_account_name, key_path)
    ee.Initialize(opt_url='https://earthengine-highvolume.googleapis.com', credentials=credentials)
