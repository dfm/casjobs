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
    def __init__(self, userid=None, password=None,
            base_url="http://casjobs.sdss.org/CasJobs/services/jobs.asmx"):
        self.userid = userid
        if userid is None:
            self.userid = int(os.environ["CASJOBS_WSID"])
        self.password = password
        if password is None:
            self.password = os.environ["CASJOBS_PW"]
        self.base_url = base_url

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

    def cancel(self, job_id):
        """
        Cancel a job.

        ## Arguments

        * `job_id` (int): The job to check.

        """
        params = {"jobid": job_id}
        self._send_request("CancelJob", params=params)

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

    def job_info(self, **kwargs):
        search = ";".join(["%s : %s"%(k, str(kwargs[k])) for k in kwargs])
        params = {"owner_wsid": self.userid, "owner_pw": self.password,
                "conditions": search, "includeSystem": False}
        r = self._send_request("GetJobs", params=params)
        results = []
        for n in minidom.parseString(r.text).getElementsByTagName("CJJob"):
            results.append({})
            for e in n.childNodes:
                if e.nodeType != e.TEXT_NODE:
                    results[-1][e.tagName] = e.firstChild.data
        return results

    def request_output(self, table, outtype):
        """
        Request the output for a given table.

        ## Arguments

        * `table` (str): The name of the table to export.
        * `outtype` (str): The type of output. Must be one of:
            CSV     - Comma Seperated Values
            DataSet - XML DataSet
            FITS    - Flexible Image Transfer System (FITS Binary)
            VOTable - XML Virtual Observatory VOTABLE

        """
        job_types = ["CSV", "DataSet", "FITS", "VOTable"]
        assert outtype in job_types
        params = {"tableName": table, "type": outtype}
        r = self._send_request("SubmitExtractJob", params=params)
        job_id = int(self._parse_single(r.text, "long"))
        return job_id

    def get_output(self, job_id, outfn):
        """
        Download an output file given the id of the output request job.

        ## Arguments

        * `job_id` (int): The id of the _output_ job.
        * `outfn` (str): The file where the output should be stored.

        """
        job_info = self.job_info(jobid=job_id)[0]

        # Make sure that the job is finished.
        status = int(job_info["Status"])
        if status != 5:
            raise Exception("The status of job %d is %d (%s)"
                    %(job_id, status, self.status_codes[status]))

        # Try to download the output file.
        remotefn = job_info["OutputLoc"]
        r = requests.get(remotefn)

        # Make sure that the request went through.
        code = r.status_code
        if code != 200:
            raise Exception("Getting file %s yielded status: %d"
                    %(remotefn, code))

        # Save the data to a file.
        f = open(outfn, "wb")
        f.write(r.content)
        f.close()

    def drop_table(self, table):
        """
        Drop a table from the MyDB context.

        ## Arguments

        * `table` (str): The name of the table to drop.

        """
        job_id = self.submit("DROP TABLE %s"%table, context="MYDB")
        status = self.monitor(job_id)
        if status[0] != 5:
            raise Exception("Couldn't drop table %s"%table)

