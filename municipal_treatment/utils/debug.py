# DEBUG_LEVELS: 
# 0: No debug output
# 1: INFO
# 2: DEBUG
DEBUG_LEVEL = 2

def debug_print(message, level=1):
    if level <= DEBUG_LEVEL:
        print(message)