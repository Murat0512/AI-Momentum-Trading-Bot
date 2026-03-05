import re

with open('risk/manager.py', 'r', encoding='utf-8') as f:
    text = f.read()

# Replace any pass with self.__init_original__()
text = text.replace('pass\n        # Handle singleton pattern safely', 'self.__init_original__()\n        # Handle singleton pattern safely')
text = text.replace('??? ', '??? ')

with open('risk/manager.py', 'w', encoding='utf-8') as f:
    f.write(text)
