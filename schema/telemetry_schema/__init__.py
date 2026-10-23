"""Runtime helpers shared by the producer and the consumer.

The code for the messages themselves is generated from the .proto files by
generate.py (into schema/generated/). The helpers don't know about any
specific field: they walk the protobuf descriptors, so they keep working when
the schema changes.
"""

from .registry import message_class

__all__ = ["message_class"]
