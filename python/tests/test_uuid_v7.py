import uuid

import aorta


def test_new_uuid_v7_returns_canonical_rfc_9562_string():
    first = aorta.new_uuid_v7()
    second = aorta.new_uuid_v7()

    assert len(first) == 36
    assert first != second
    assert first == str(uuid.UUID(first))
    assert uuid.UUID(first).version == 7
    assert uuid.UUID(first).variant == uuid.RFC_4122
