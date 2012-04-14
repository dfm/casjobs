"""
Interface to CasJobs for Humans.(TM)

"""

import time
import os
import logging
from xml.dom import minidom

import requests

__all__ = ["CasJobs"]

class CasJobs(object):
    """
    Wrapper around the CasJobs service.

    ## Keyword Arguments

    * `userid` (int): The WSID from your CasJobs profile. If this is not
      provided, it should be in your environment variable `CASJOBS_WSID`.
    * `password` (str): Your super-secret CasJobs password. It can also be
      provided by the `CASJOBS_PW` environment variable.
    * `base_url` (str): The base URL that you'd like to use depending on the
      service that you're accessing. This module has only really been tested
      with `http://casjobs.sdss.org/CasJobs/services/jobs.asmx`.

    """
    def __init__(self, **kwargs):
        self.userid = kwargs.pop("userid",
                int(os.environ.get("CASJOBS_WSID", 0)))
        self.password = kwargs.pop("password",
                os.environ.get("CASJOBS_PW", None))
        self.base_url = kwargs.pop("base_url",
                "http://casjobs.sdss.org/CasJobs/services/jobs.asmx")

        # MAGIC: job status ids.
        self.status_codes = ("ready", "started", "canceling", "cancelled",
                             "failed", "finished")

    def _send_request(self, job_type, params={}):
        """
        Construct and submit a structured/authenticated request.

        ## Arguments

        * `job_type` (str): The job type identifier to use.

        ## Keyword Arguments

        * `params` (dict): Any additional entries to include in the POST
          request.

        ## Returns

        * `r` (requests.Response): The response from the server.

        """
        params["wsid"] = params.get("wsid", self.userid)
        params["pw"]   = params.get("pw", self.password)

        path = os.path.join(self.base_url, job_type)
        r = requests.get(path, params=params)

        code = r.status_code
        if code != 200:
            raise Exception("%s failed with status: %d"%(job_type, code))

        return r

    def _parse_single(self, text, tagname):
        """
        A hack to get the content of the XML responses from the CAS server.

        ## Arguments

        * `text` (str): The XML string to parse.
        * `tagname` (str): The tag that contains the info that we want.

        ## Returns

        * `content` (str): The contents of the tag.

        """
        return minidom.parseString(text)\
                .getElementsByTagName(tagname)[0].firstChild.data

    def submit(self, q, context="DR7", task_name="casjobs", estimate=30):
        """
        Submit a job to CasJobs.

        ## Arguments

        * `q` (str): The SQL query.

        ## Keyword Arguments

        * `task_name` (str): The task name.
        * `estimate` (int): Estimate.

        ## Returns

        * `job_id` (int): The submission ID.

        """
        params = {"qry": q, "context": context, "taskname": task_name,
                  "estimate": estimate}
        r = self._send_request("SubmitJob", params=params)
        job_id = int(self._parse_single(r.text, "long"))
        return job_id

    def status(self, job_id):
        """
        Check the status of a job.

        ## Arguments

        * `job_id` (int): The job to check.

        ## Returns

        * `code` (int): The status.
        * `status` (str): The human-readable name of the current status.

        """
        params = {"jobid": job_id}
        r = self._send_request("GetJobStatus", params=params)
        status = int(self._parse_single(r.text, "int"))
        return status, self.status_codes[status]

    def monitor(self, job_id, timeout=5):
        """
        Monitor the status of a job.

        ## Arguments

        * `job_id` (int): The job to check.

        ## Returns

        * `code` (int): The status.
        * `status` (str): The human-readable name of the current status.

        """
        while True:
            status = self.status(job_id)
            logging.info("Monitoring job: %d - Status: %d, %s"
                    %(job_id, status[0], status[1]))
            if status[0] in [3, 4, 5]:
                return status
            time.sleep(5)

if __name__ == '__main__':
    jobs = CasJobs()
    job_id = jobs.submit("""SELECT *
INTO mydb.pisces2
FROM Stripe82..Field AS p
WHERE p.mjd_g > 0 AND p.ramin < 355 AND p.ramax > 355""")

    status = jobs.monitor(job_id)
    print status

