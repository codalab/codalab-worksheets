##Notes on using PyLint

PyLint (http://pylint.org, http://docs.pylint.org/index.html) is a code checker that detects errors in Python code, helping to achieve a consistent coding standard. When you run PyLint on a .py file or module, it checks your code against an assortment of standards and generates a report.

From the virtual environment, PyLint installs with pip:
```
pip install pylint
```
At the time of writing, pip install PyLint 1.0.0 which has a severe issue on Windows with trailing whitespace: see https://bitbucket.org/logilab/pylint/commits/02db08561a8e. A workaround is to manually update `format.py` which is located under `Lib\site-packages\pylint\checkers`. Alternatively, use the `--disable=trailing-whitespace` option.

## Using PyLint with CodaLab
To run PyLint against the entire CodaLab project: 
1. Navigate to \codalab\codalab (wherever that is located on your local system).
1. Run `pylint codalab` to scan the entire project.


## PyLint Tips
- To output your report in HTML format, use the `-f html` option.
- To pipe the report output into a file, use this syntax:
`pylint codalab -f html > results.html`
- Use the `-rn` option to omit the report tables.

## Configure PyLint Rules

PyLint's rules are configurable. To generate a default config file:
```
pylint --generate-rcfile > .pylint-conf
```
To use the generated config (assuming `.pylint-conf` is located in the current directory):
```
pylint –-rcfile=.pylint-conf <module or package>
```

Below is a set of customizations that will make working with PyLint a bit easier.

* Output format. PyLint can format its output multiple ways. I find the HTML format to be convenient.
```
output-format=html
```
* Line length. PyLint enforces an 80 character line length by default. It is on the short side.
```
max-line-length=120
```
* Number of methods on a class. PyLint likes at least two methods on a class. It also has an upper-limit which is too low when writing a class derived from TestCase for example.
```
min-public-methods=1
max-public-methods=100
```
* Dealing with Django generated members such as `objects`:
```
generated-members=REQUEST,acl_users,aq_parent,objects,^.DoesNotExist$
```
* The `disable` flag allows to turn off rules using their ID:
```
disable=R0921
```
    * R0921: Abstract class not references (http://stackoverflow.com/questions/8261526/how-to-fix-pylint-warning-abstract-class-not-referenced).


###Related tools

pep8: https://pypi.python.org/pypi/pep8/. Compared to PyLint, pep8 feels more lightweight in its feedback and probably too much so. 

