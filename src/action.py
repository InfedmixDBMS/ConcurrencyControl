from enum import Enum

class Action(Enum):
    READ = 'read'
    WRITE = 'write'
    COMMIT = 'commit'
