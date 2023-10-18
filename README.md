## zimran-events

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

# < 0.4.0

from zimran.events import Consumer
from zimran.events.dto import Exchange

consumer = Consumer(service_name='my-service', broker_url='')
consumer.add_event_handler(
            name='routing-key',
            handler=handler_func,
            exchange=Exchange(
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



# >= 0.4.0 version


from zimran.events.routing import Router
from zimran.events.consumer AsyncConsumer


router = Router()

@router.event_handler('routing-key')
async def handler(message: aio_pika.IncomingMessage):
  pass


router.add_event_handler('routing-key', some_handler)



async def main():
  consumer = AsyncConsumer(..., router=router)

  await consumer.run()
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
