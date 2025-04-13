import uuid

# implement locking functions

# check if uuid is valid -- return boolean
def validate_uuid(uuid_value):
    """validates uuid
    
    return: boolean
    """
    # UUID is invalid if it's not a string
    if not isinstance(uuid_value, str):
        return False
    
    # If uuid.UUID raises an exception, then the uuid is invalid
    try:
        uuid_check = uuid.UUID(uuid_value)
    except (ValueError):
        return False
    return True

