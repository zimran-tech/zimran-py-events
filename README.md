## zimran-contrib

The `zimran-py-events` module provides AMQP interface

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg?color=green&style=plastic)](https://opensource.org/licenses/MIT)
![code size](<https://img.shields.io/github/languages/code-size/zimran-tech/zimran-py-events?style=plastic>)

## Installation

```bash
pip install zimran-events
```

## Usage example

**Producer**

```python

from zimran.events import AsyncProducer

producer = AsyncProducer(broker_url='')
await producer.connect()

# message publishing
await producer.publish('some.event.routing', {'msg': 'hello, world'})
```

**Consumer**

```python

from zimran.events import Consumer
from zimran.events.schemas import ExchangeScheme

consumer = Consumer(service_name='my-service', broker_url='')
consumer.add_event_handler(
            name='routing-key',
            handler=handler_func,
            exchange=ExchangeScheme(
                name='exchange-name',
                type='exchange-type',
                durable=True,
            )
           )

# or

from zimran.events import Consumer

consumer = Consumer(service_name='my-service', broker_url='')

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
