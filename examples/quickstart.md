# Codalab CLI Quickstart

Install the Codalab CLI:
```
$ pip install codalab_cli
```

Create a program bundle:
```
$ codalab upload program *program*
```

Create a data bundle:
```
$ codalab upload dataset *dataset*
```

Create a Run bundle:
```
$ codalab run *program* *dataset* 'kwargs'
```

Run Codalab worker to execute:
```
$ codalab worker
```
