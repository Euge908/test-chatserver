import json
import time
import uuid
from dataclasses import asdict, dataclass


@dataclass
class ChatMessage:
    id: str
    client_id: int
    content: str
    ts: float

    @classmethod
    def create(cls, client_id: int, content: str) -> "ChatMessage":
        return cls(id=str(uuid.uuid4()), client_id=client_id, content=content, ts=time.time())

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, s: str) -> "ChatMessage":
        return cls(**json.loads(s))
