# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: message.proto

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='message.proto',
  package='number',
  syntax='proto3',
  serialized_options=None,
  serialized_pb=b'\n\rmessage.proto\x12\x06number\"Y\n\x0cNumberBuffer\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x11\n\ttimestamp\x18\x02 \x01(\x03\x12\x0c\n\x04name\x18\x03 \x01(\t\x12\x0e\n\x06number\x18\x04 \x01(\x05\x12\x0c\n\x04type\x18\x05 \x01(\tb\x06proto3'
)




_NUMBERBUFFER = _descriptor.Descriptor(
  name='NumberBuffer',
  full_name='number.NumberBuffer',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='id', full_name='number.NumberBuffer.id', index=0,
      number=1, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='timestamp', full_name='number.NumberBuffer.timestamp', index=1,
      number=2, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='name', full_name='number.NumberBuffer.name', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='number', full_name='number.NumberBuffer.number', index=3,
      number=4, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='type', full_name='number.NumberBuffer.type', index=4,
      number=5, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=25,
  serialized_end=114,
)

DESCRIPTOR.message_types_by_name['NumberBuffer'] = _NUMBERBUFFER
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

NumberBuffer = _reflection.GeneratedProtocolMessageType('NumberBuffer', (_message.Message,), {
  'DESCRIPTOR' : _NUMBERBUFFER,
  '__module__' : 'message_pb2'
  # @@protoc_insertion_point(class_scope:number.NumberBuffer)
  })
_sym_db.RegisterMessage(NumberBuffer)


# @@protoc_insertion_point(module_scope)
