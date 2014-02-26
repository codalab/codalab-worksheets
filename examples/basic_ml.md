$ cl new basic_ml
$ cl upload program weka
$ cl upload dataset vote
$ cl run weka vote 'program/split input output 4'




- Switched to worksheet basic_ml.

- 0x3c5dbf07dac64a24a4efd5abd96e7327

- 0xdc5731838047420da63e5b7eb7a2ee9a

- 0x2d71349a36d94256b5d52178602d59d8 (split 3)



$ cl rm split
Traceback (most recent call last):
  File "/usr/local/share/python//codalab_client.py", line 13, in <module>
    cli.do_command(args)
  File "/usr/local/Cellar/python/2.7.3/Frameworks/Python.framework/Versions/2.7/lib/python2.7/site-packages/codalab/lib/bundle_cli.py", line 183, in do_command
    command_fn(remaining_args, parser)
  File "/usr/local/Cellar/python/2.7.3/Frameworks/Python.framework/Versions/2.7/lib/python2.7/site-packages/codalab/lib/bundle_cli.py", line 305, in do_rm_command
    self.client.delete(args.bundle_spec, args.force)
  File "/usr/local/Cellar/python/2.7.3/Frameworks/Python.framework/Versions/2.7/lib/python2.7/site-packages/codalab/client/local_bundle_client.py", line 139, in delete
    '\n  '.join(str(child) for child in child_worksheets),
codalab.common.UsageError: Worksheets depend on split:
  Worksheet(uuid=u'0xbe480ce0daaa412b9871b1590a6e2219', name=u'basic_ml')

