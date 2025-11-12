class Response:
    def __init__(self, allowed: bool, transaction_id: int):
        self.allowed = allowed
        self.transaction_id = transaction_id
