import warnings
from urllib3.exceptions import InsecureRequestWarning
import json
import time
from typing import Union, Tuple
from pathlib import Path
from io import BytesIO

import requests

# # todo: wrap filter with enable disable around relevant api calls
# warnings.simplefilter("ignore", InsecureRequestWarning)


__DEFAULT_JENKINS_URL__ = "http://localhost:8080"
__QUEUE_SLEEP_TIME_TIMEOUE__ = 5 * 60
__JOB_EXECUTION_DEFAULT_TIMEOUT__ = 24 * 60 * 60
__PULLIN_DELAY__ = 1


class JenkinsClient:
    """
    simplified jenkins client.
    wraps api calls
    doesn't keep stats
    """

    def __init__(
        self,
        auth: Tuple[str, str] = None,
        jenkins_url: str = __DEFAULT_JENKINS_URL__,
        verify=None,
    ) -> None:
        self._base_url = jenkins_url
        self._auth = auth
        self._verify = verify

    def get(self, *args, **kwargs):
        assert "verify" not in kwargs
        assert "auth" not in kwargs
        res = requests.get(*args, **kwargs, auth=self._auth, verify=self._verify)

        return res

    def get_build_info(self, job_name, build_number):
        return {
            "job": f"{self._base_url}/job/{job_name}/{build_number}",
            "console": f"{self._base_url}/job/{job_name}/{build_number}/console",
            "consoleFull": f"{self._base_url}/job/{job_name}/{build_number}/consoleFull",
            "consoleText": f"{self._base_url}/job/{job_name}/{build_number}/consoleText",
        }

    def pull_in_response(self, url, key, timeout, log=True):
        """pullin on url until response json contains key or timeout expires

        Returns:
            any: response.json()['key'] or None if timeout
        """
        start_time = time.time()
        result = None
        while True:
            response = self.get(url)
            if response.status_code not in [200, 201]:
                raise Exception(
                    f"Failed jenkins request - code: {response.status_code}, url: {url}, text: {response.text}"
                )

            response_json = response.json()
            result = response_json.get(key)
            if result is not None:
                break

            if timeout is None:
                break

            if time.time() - start_time > timeout:
                print(f"job status pulling timout ({timeout} seconds) exceeded")
                break

            if log:
                print(f"pulling '{url}' on '{key}'")
            time.sleep(__PULLIN_DELAY__)

        return result

    @staticmethod
    def handle_file(k, f):
        if isinstance(f, Path):
            return open(f, "rb")
        elif isinstance(f, dict):
            return BytesIO(json.dumps(f, indent=4).encode())
        elif isinstance(f, BytesIO):
            return
        else:
            raise RuntimeError(f"unsupported file type for '{k}', type: {type(f).__name__}.")

    def run_jenkins_job(
        self,
        job_name: str,
        params: dict = None,
        files: Union[None, dict] = None,
        build="buildWithParameters",
        timeout=__QUEUE_SLEEP_TIME_TIMEOUE__,
        log=False,
    ) -> str:
        if params is None:
            params = dict()

        if files is None:
            files = {}

        job_build_url = f"{self._base_url}/job/{job_name}/{build}"

        # handle files
        files_data = {k: self.handle_file(k, f) for k, f in files.items()}

        # send request
        response = requests.post(job_build_url, data=params, files=files_data, auth=self._auth, verify=False)
        # close files
        [f.close() for f in files_data.values()]

        if response.status_code not in [200, 201]:
            raise Exception(
                f"Failed Jenkins run job request - code: {response.status_code}, url: {job_build_url}, text: {response.text}"
            )  # nopep8

        queue_item_url = f"{response.headers['location']}api/json"
        executable = self.pull_in_response(queue_item_url, "executable", timeout=timeout, log=log)

        build_number = executable["number"]

        return build_number

    def get_job_result(self, job_name: str, build_number) -> str:
        job_info_url = f"{self._protocol}://{self._base_url}/job/{job_name}/{build_number}/api/json"

        response = self.get(job_info_url)
        if response.status_code not in [200, 201]:
            raise Exception(
                f"Failed jenkins request - code: {response.status_code}, url: {job_info_url}, body: {response.body}"
            )

        response_json = response.json()
        result = response_json.get("result")

        return result

    def wait_job_result(self, job_name: str, build_number, timeout=__JOB_EXECUTION_DEFAULT_TIMEOUT__) -> str:
        job_info_url = f"{self._protocol}://{self._base_url}/job/{job_name}/{build_number}/api/json"
        result = self.pull_in_response(job_info_url, "result", timeout)

        return result
