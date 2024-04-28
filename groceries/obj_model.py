import dataclasses
from decimal import Decimal
from typing import Optional


@dataclasses.dataclass
class ListItem:
    name: str
    index: int
    in_cart: bool
    purchase_price: Optional[Decimal]
