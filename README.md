## zimran-contrib

The `zimran-py-events` module provides reusable core parts of code

## Installation

```bash
pip install zimran-events
```

## Usage example

**Producer**

```python

from zimran.events import Producer

producer = Producer(broker_url='')
producer.publish('some.event.routing', {'msg': 'hello, world'})
```

**Consumer**

```python

from zimran.events import Consumer

consumer = Consumer(service_name='my-service', broker_url='')
consumer.add_event_handler('routing-key', handler_func)

# or

from zimran.events import Consumer

consumer = Consumer(service_name='my-service', broker_url='')
consumer.add_event_handler('routing-key', handler_func)

@consumer.event_handler('routing-key')
def handler_func(**kwargs):
  ...

```

## Code

The code and issue tracker are hosted on GitHub: <https://github.com/zimran-tech/zimran-py-events.git>

## Features

- AMQP interfaces

### For contributors

**Setting up development environment**

Clone the project:

```bash
git clone https://github.com/zimran-tech/zimran-py-events.git
cd zimran-py-events
```

Create a new virtualenv:

```bash
python3 -m venv venv
source venv/bin/activate
```
