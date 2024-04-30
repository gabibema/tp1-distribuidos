from .workers import Aggregate, Filter, Map, Router, wait_rabbitmq
from .dynamic_workers import DynamicAggregate, DynamicFilter, DynamicRouter
from .gateway import Proxy, Sender

MAX_KEY_LENGTH = 255
