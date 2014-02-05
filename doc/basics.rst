Basic Usage
===========
The `pyrakoon` library provides the building blocks to create an Arakoon_
client library in Python_. It contains the required data types used in the
client protocol, including serialization and parsing routines. It also includes
descriptions of the available operations.

.. _Arakoon: http://arakoon.org
.. _Python: http://www.python.org

The code is not bound to a specific method of communicating with Arakoon nodes
though. This is abstracted, and left up to the user to implement according to
specific needs and communication mechanisms.

Socket Handling
---------------
To provide an implementation of the abstract communication mechanism, the
:class:`~pyrakoon.client.AbstractClient` interface must be fulfilled. A very
basic implementation, which can be used to communicate with a single Arakoon
node (i.e. no master failover or reconnection is provided) using blocking socket
calls is provided by :class:`~pyrakoon.client.SocketClient`.

.. warning::

    `SocketClient` should only be used for (manual) testing purposes. Due to
    the lack of good exception handling, timeouts,... it should not be used in
    real-world code.

Mixins
------
When given an :class:`~pyrakoon.client.AbstractClient` implementation, this
doesn't give access to actual client operations. Whilst it's possible to create
instances of the calls as defined in :mod:`pyrakoon.protocol` and related
modules and pass these through :meth:`~pyrakoon.client.AbstractClient._process`,
this is rather clumsy.

To provide a uniform interface, not bound to a specific
:class:`~pyrakoon.client.AbstractClient` implementation, a couple of mixins are
provided, which expose client operations in a user-friendly way. Several mixins
are available:

* :class:`pyrakoon.client.ClientMixin` for standard client operations.
* :class:`pyrakoon.client.admin.ClientMixin` for administrative operations.

These can be combined with an :class:`~pyrakoon.client.AbstractClient`
implementation and used as-is. Here's an example using
:class:`~pyrakoon.client.SocketClient`, mixing in
:class:`pyrakoon.client.ClientMixin` and
:class:`pyrakoon.client.admin.ClientMixin`:

.. doctest::

    >>> from pyrakoon import client
    >>> from pyrakoon.client import admin

    >>> class Client(client.SocketClient, client.ClientMixin, admin.ClientMixin):
    ...     '''An Arakoon client'''

    >>> c = Client(('localhost', 4000), 'ricky')
    >>> c.connect()
    >>> c.set('key', 'value') # from client.ClientMixin
    >>> c.collapse_tlogs(4) # from admin.ClientMixin
    []

.. testcleanup::

    c.delete('key')
