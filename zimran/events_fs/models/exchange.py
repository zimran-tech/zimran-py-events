from dataclasses import dataclass, field
from typing import Dict, Optional

@dataclass
class Exchange:
    name: str
    type: str = "direct"
    durable: bool = True
    arguments: Optional[Dict] = field(default_factory=dict)
    ignore_unroutable: bool = False
    
    def __post_init__(self):
        if self.arguments is None:
            self.arguments = {}