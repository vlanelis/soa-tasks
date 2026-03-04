from pydantic import BaseModel, StrictInt, StrictStr

from marketplace.models.user_role import UserRole


class TokenModel(BaseModel):
    user_id: int
    role: UserRole
