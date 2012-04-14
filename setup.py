from distutils.core import setup

desc = """This basically does the same thing as
python-casjobs (https://github.com/cosmonaut/python-casjobs) but it only
depends on Requests (http://python-requests.org) which makes it more useful
in my opinion."""

setup(
    name="casjobs",
    version="0.0.1",
    author="Daniel Foreman-Mackey",
    author_email="danfm@nyu.edu",
    packages=["casjobs"],
    url="https://github.com/dfm/casjobs",
    license="MIT",
    description="An interface to CasJobs for Humans.",
    long_description=desc,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
    ],
)

