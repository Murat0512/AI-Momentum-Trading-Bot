with open('infrastructure/ib_connection.py', 'r') as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    if "def connect(self):" in line:
        lines.insert(i, "    @property\n    def is_connected(self):\n        return self.ib.isConnected()\n\n")
        break

with open('infrastructure/ib_connection.py', 'w') as f:
    f.writelines(lines)
