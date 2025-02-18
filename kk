Traceback (most recent call last):
  File "/usr/lib64/python3.9/runpy.py", line 197, in _run_module_as_main
    return _run_code(code, main_globals, None,
  File "/usr/lib64/python3.9/runpy.py", line 87, in _run_code
    exec(code, run_globals)
  File "/home/udayka/python/vrepair_ticket_opened_norm_v0.py", line 17, in <module>
    @beam.typehints.with_output_types(beam.typehints.NamedTupleSequence)
AttributeError: module 'apache_beam.typehints' has no attribute 'NamedTupleSequence'
