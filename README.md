This basically does the same thing as
[python-casjobs](https://github.com/cosmonaut/python-casjobs) but it only
depends on [Requests](http://python-requests.org) which makes it more useful
in my opinion.

## Usage

Here's how you might get the fields in Stripe 82 that hit the Pisces
overdensity:

```python
import casjobs

query = """SELECT *
INTO mydb.pisces2
FROM Stripe82..Field AS p
WHERE p.mjd_g > 0 AND p.ramin < 355 AND p.ramax > 355
"""

jobs = CasJobs()
job_id = jobs.submit(query)
status = jobs.monitor(job_id)
print status
```

## License

MIT

