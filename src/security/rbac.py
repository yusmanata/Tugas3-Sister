class RBACManager:
    # Defined roles and their permissions
    ROLES = {
        "admin": ["read", "write", "manage_nodes", "delete"],
        "user": ["read", "write"],
        "guest": ["read"]
    }
    
    # Simple user database for the simulation
    USERS = {
        "client_A": "admin",
        "client_B": "user",
        "client_C": "guest"
    }

    @classmethod
    def get_user_role(cls, user_id: str) -> str:
        return cls.USERS.get(user_id, "guest")

    @classmethod
    def check_permission(cls, user_id: str, action: str) -> bool:
        """
        Check if the user_id has permission to perform the action.
        """
        role = cls.get_user_role(user_id)
        permissions = cls.ROLES.get(role, [])
        return action in permissions

    @classmethod
    def require_permission(cls, user_id: str, action: str):
        """
        Raises an exception if the user does not have permission.
        """
        if not cls.check_permission(user_id, action):
            raise PermissionError(f"Access Denied: User '{user_id}' with role '{cls.get_user_role(user_id)}' cannot perform action '{action}'")
