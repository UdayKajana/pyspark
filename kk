Traceback (most recent call last):
  File "/usr/lib64/python3.9/runpy.py", line 197, in _run_module_as_main
    return _run_code(code, main_globals, None,
  File "/usr/lib64/python3.9/runpy.py", line 87, in _run_code
    exec(code, run_globals)
  File "/home/udayka/python/vrepair_ticket_opened_norm_v0.py", line 221, in <module>
    run(known_args, beam_args)
  File "/home/udayka/python/vrepair_ticket_opened_norm_v0.py", line 187, in run
    | "Batch Elements" >> create_batch_transform()
  File "/home/udayka/python/vrepair_ticket_opened_norm_v0.py", line 158, in create_batch_transform
    coder=beam.coders.IterableCoder(beam.coders.RowCoder(ConvertToSpannerMutation))
  File "/apps/opt/application/vznet/pyvenv39/lib64/python3.9/site-packages/apache_beam/coders/row_coder.py", line 65, in __init__
    self._type_hint = named_tuple_from_schema(self.schema)
  File "/apps/opt/application/vznet/pyvenv39/lib64/python3.9/site-packages/apache_beam/typehints/schemas.py", line 590, in named_tuple_from_schema
    return SchemaTranslation(
  File "/apps/opt/application/vznet/pyvenv39/lib64/python3.9/site-packages/apache_beam/typehints/schemas.py", line 542, in named_tuple_from_schema
    type_name = 'BeamSchema_{}'.format(schema.id.replace('-', '_'))
AttributeError: type object 'ConvertToSpannerMutation' has no attribute 'id'
