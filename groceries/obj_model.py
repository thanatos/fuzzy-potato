import dataclasses
from decimal import Decimal
from typing import List, Optional


@dataclasses.dataclass
class ListItem:
    name: str
    index: int
    in_cart: bool
    purchase_price: Optional[Decimal]


@dataclasses.dataclass
class GroceryList:
    created_at: str
    items: List[ListItem]
