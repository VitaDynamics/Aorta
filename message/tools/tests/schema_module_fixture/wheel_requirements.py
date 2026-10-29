"""Pin composition fixtures to the SDK/base wheels actually built with them."""
from email import message_from_bytes
from pathlib import Path
import sys
import zipfile


metadata = []
for path in sys.argv[1:3]:
    with zipfile.ZipFile(path) as wheel:
        name = next(n for n in wheel.namelist() if n.endswith('.dist-info/METADATA'))
        metadata.append(message_from_bytes(wheel.read(name)))
requirements = [f"{m['Name'].replace('_', '-')} == {m['Version']}" for m in metadata]
requirements.extend(metadata[1].get_all('Requires-Dist', []))
Path(sys.argv[3]).write_text('\n'.join(requirements) + '\n')
