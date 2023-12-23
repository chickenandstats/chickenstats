import requests
from requests.adapters import HTTPAdapter
from requests import ConnectionError, ReadTimeout, ConnectTimeout, HTTPError, Timeout
import urllib3

from tqdm.auto import tqdm

from datetime import datetime

import numpy as np

############################################## Requests functions & classes ##############################################


# This function & the timeout class are used for scraping throughout
class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.timeout = 3

        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]

            del kwargs["timeout"]

        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")

        if timeout is None:
            kwargs["timeout"] = self.timeout

        return super().send(request, **kwargs)


def s_session():
    """Creates a requests Session object using the HTTPAdapter from above"""

    s = requests.Session()

    retry = urllib3.Retry(
        total=5,
        backoff_factor=1,
        respect_retry_after_header=False,
        status_forcelist=[54, 60, 401, 403, 404, 408, 429, 500, 502, 503, 504],
    )

    adapter = TimeoutHTTPAdapter(max_retries=retry, timeout=3)

    s.mount("http://", adapter)

    s.mount("https://", adapter)

    return s


############################################# General helper functions ##############################################


def convert_to_list(obj, object_type):
    """If the object is not a list, converts the object to a list of length one"""

    if (
        type(obj) is str
        or isinstance(obj, (int, np.integer)) == True
        or isinstance(obj, (float, np.float)) == True
    ):
        obj = [obj]

    else:
        try:
            obj = [x for x in obj]

        except:
            raise Exception(
                f"'{obj}' not a supported {object_type} or range of {object_type}s"
            )

    return obj


def update_pbar(pbar, message):
    """Updates progress bar output"""

    pbar.set_description(f"{message}".upper())

    ## Adding current time to the progress bar

    now = datetime.now()

    current_time = now.strftime("%H:%M:%S")

    postfix_str = f"{current_time}"

    pbar.set_postfix_str(postfix_str)
