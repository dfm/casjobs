"""
Interface to CasJobs for Humans.(TM)

"""

__all__ = ["CasJobs"]

import time
import os
import logging
import re
from xml.dom import minidom
import requests

try:
    from html import unescape
except ImportError:
    # Python 2.7
    import htmllib
    def unescape(s):
        p = htmllib.HTMLParser(None)
        p.save_bgn()
        p.feed(s)
        return p.save_end()


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
      with ``http://casjobs.sdss.org/CasJobs/services/jobs.asmx`` (the original
      CasJobs for SDSS I and II, closed Jul 31, 2014), as well as the default
      (maintained by SDSS III).
    * `request_type` (str): The type of HTTP request to use to access the
      CasJobs services.  Can be either 'GET' or 'POST'.  Typically you
      may as well stick with 'GET', unless you want to submit some long
      queries (>~2000 characters or something like that).  In that case,
      you'll need 'POST' because it has no length limit.
    * `context` (str): Default context that is used for queries.

    """
    def __init__(self, userid=None, password=None,
                 base_url="http://skyserver.sdss3.org/casjobs/services/jobs.asmx",
                 request_type="GET", context="DR7"):
        self.userid = userid
        if userid is None:
            self.userid = int(os.environ["CASJOBS_WSID"])
        self.password = password
        if password is None:
            self.password = os.environ["CASJOBS_PW"]
        self.base_url = base_url
        self.context = context

        # MAGIC: job status ids.
        self.status_codes = ("ready", "started", "canceling", "cancelled",
                             "failed", "finished")

        if request_type.upper() not in ('GET', 'POST'):
            raise ValueError('`request_type` can only be either "GET" or "POST"')
        self.request_type = request_type.upper()

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

        path = self.base_url + '/' + job_type
        if self.request_type == 'GET':
            r = requests.get(path, params=params)
        elif self.request_type == 'POST':
            r = requests.post(path, data=params)
        else:
            raise ValueError('`request_type` is invalid!')

        code = r.status_code
        if code != 200:
            if hasattr(r,'text') and r.text:
                msg = self._parse_error(r.text)
                raise Exception("%s failed with status: %d\n%s"%(job_type, code, msg))
            else:
                raise Exception("%s failed with status: %d (no additional information)"%(job_type, code))

        return r

    def _parse_error(self, text, maxlines=2):
        """
        Extract SQL error message from Java traceback

        If the SQL error is not found, the first maxlines lines of the
        (long) Java traceback are returned.

        ### Arguments

        * `text` (str): The request response (usually a Java traceback)
        * `maxlines` (int): Maximum lines to return if not a SQL error

        ## Returns

        * `msg` (str): Error message with HTML entities decoded

        """
        pat = re.compile(r'System.Exception: *(?P<msg>.*) *--->',re.DOTALL)
        mm = pat.search(text)
        if mm:
            msg = mm.group('msg')
        else:
            msg = '\n'.join(text.split('\n')[:maxlines])
        return unescape(msg)

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

    def quick(self, q, context=None, task_name="quickie", system=False):
        """
        Run a quick job.

        ## Arguments

        * `q` (str): The SQL query.

        ## Keyword Arguments

        * `context` (str): Casjobs context used for this query.
        * `task_name` (str): The task name.
        * `system` (bool) : Whether or not to run this job as a system job (not
          visible in the web UI or history)

        ## Returns

        * `results` (str): The result of the job as a long string.

        """
        if not context:
            context = self.context
        params = {"qry": q, "context": context, "taskname": task_name,
                "isSystem": system}
        r = self._send_request("ExecuteQuickJob", params=params)
        return self._parse_single(r.text, "string")

    def submit(self, q, context=None, task_name="casjobs", estimate=30):
        """
        Submit a job to CasJobs.

        ## Arguments

        * `q` (str): The SQL query.

        ## Keyword Arguments

        * `context` (str): Casjobs context used for this query.
        * `task_name` (str): The task name.
        * `estimate` (int): Estimate of the time this job will take (in minutes).

        ## Returns

        * `job_id` (int): The submission ID.

        """
        if not context:
            context = self.context
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
        * `timeout` (float): The time to wait between checks (in sec).

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
            time.sleep(timeout)

    def job_info(self, **kwargs):
        """
        Get the information about the jobs returned by a particular search.
        See the [GetJobs][] documentation for more info.

        [GetJobs]: http://casjobs.sdss.org/casjobs/services/jobs.asmx?op=GetJobs

        """
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
            May also be a file-like object with a 'write' method.

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
        try:
            outfn.write(r.content)
        except AttributeError:
            f = open(outfn, "wb")
            f.write(r.content)
            f.close()

    def request_and_get_output(self, table, outtype, outfn):
        """
        Shorthand for requesting an output file and then downloading it when
        ready.

        ## Arguments

        * `table` (str): The name of the table to export.
        * `outtype` (str): The type of output. Must be one of:
            CSV     - Comma Seperated Values
            DataSet - XML DataSet
            FITS    - Flexible Image Transfer System (FITS Binary)
            VOTable - XML Virtual Observatory VOTABLE
        * `outfn` (str): The file where the output should be stored.
            May also be a file-like object with a 'write' method.

        """
        job_id = self.request_output(table, outtype)
        status = self.monitor(job_id)
        if status[0] != 5:
            raise Exception("Output request failed.")
        self.get_output(job_id, outfn)

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

    def count(self, q):
        """
        Shorthand for counting the results of a specific query.

        ## Arguments

        * `q` (str): The query to count. This will be executed as:
          `"SELECT COUNT(*) %s" % q`.

        ## Returns

        * `count` (int): The resulting count.

        """
        q = "SELECT COUNT(*) %s"%q
        return int(self.quick(q).split("\n")[1])

    def list_tables(self):
        """
        Lists the tables in mydb.

        ## Returns

        * `tables` (list): A list of strings with all the table names from mydb.
        """
        q = 'SELECT Distinct TABLE_NAME FROM information_schema.TABLES'
        res = self.quick(q, context='MYDB', task_name='listtables', system=True)
        # the first line is a header and the last is always empty
        # also, the table names have " as the first and last characters
        return [l[1:-1]for l in res.split('\n')[1:-1]]
