#!/usr/bin/env python3
"""EDU external-root fixture — Python G3 cells: message, service
and action, each IMPORTED and USED, not merely built.

The single dependency `:edu_msgs_py` is the `py_library` wrapper around the
merged tree: a tree artifact alone carries no `PyInfo` and
could not be a `py_test` dep at all, so "the tree built" would have been a
weaker claim than "a consumer can import the message class".
"""

from __future__ import annotations

import hashlib
import unittest

import flatbuffers

# Generated message classes (flatc `--gen-object-api`), namespace `edu.demo`.
from edu.demo.EduDemoJobGetResultResponse import EduDemoJobGetResultResponse
from edu.demo.EduDemoJobFeedbackData import EduDemoJobFeedbackDataT
from edu.demo.EduDemoJobGoal import EduDemoJobGoalT
from edu.demo.EduDemoJobResult import EduDemoJobResultT
from edu.demo.EduDemoServiceRequest import (
    EduDemoServiceRequest,
    EduDemoServiceRequestT,
)
from edu.demo.EduDemoText import EduDemoText, EduDemoTextT
from edu.demo.EduDemoUnit import EduDemoUnit

# Generated schema-meta modules: `SCHEMA_BFBS` / `SCHEMA_HASH_HEX` /
# `ROOT_TYPE_NAME` / `FIXED_SIZE` / `HAS_AORTA_HEADER`, plus a re-export of the
# message classes (one dep, one import).
import edu_demo_job_schema_meta as job_meta
import edu_demo_service_schema_meta as service_meta
import edu_demo_text_schema_meta as text_meta

from aorta import errors
from aorta.action import ActionReplyCode, ActionStatus, FlatbuffersActionCodec
from aorta.options import inject_service_schema


class MessageCellTest(unittest.TestCase):
    """G3 cell: Python × message."""

    def test_schema_meta_protocol_is_complete(self):
        self.assertEqual(64, len(text_meta.SCHEMA_HASH_HEX))
        self.assertEqual(
            text_meta.SCHEMA_HASH_HEX,
            hashlib.sha256(text_meta.SCHEMA_BFBS).hexdigest(),
        )
        self.assertEqual("edu.demo.EduDemoText", text_meta.ROOT_TYPE_NAME)
        self.assertTrue(text_meta.HAS_AORTA_HEADER)
        self.assertIsNone(text_meta.FIXED_SIZE)

    def test_object_api_round_trip(self):
        message = EduDemoTextT()
        message.text = "hello external root"
        message.count = 7
        builder = flatbuffers.Builder(64)
        builder.Finish(message.Pack(builder))
        payload = bytes(builder.Output())

        decoded = EduDemoText.GetRootAs(payload, 0)
        self.assertEqual(b"hello external root", decoded.Text())
        self.assertEqual(7, decoded.Count())


class ServiceCellTest(unittest.TestCase):
    """G3 cell: Python × service."""

    def test_typed_service_schema_injection(self):
        options = inject_service_schema(None, service_meta, service_meta)
        self.assertEqual(service_meta.SCHEMA_HASH_HEX, options.service_schema_hash_hex)
        self.assertEqual(service_meta.SCHEMA_BFBS, options.service_schema_bfbs)
        self.assertEqual(
            "edu.demo.EduDemoServiceRequest", options.request_root_type
        )
        self.assertEqual(
            "edu.demo.EduDemoServiceRequest", options.response_root_type
        )

    def test_request_and_response_share_one_schema_identity(self):
        # Both root types come from ONE .fbs, hence one bfbs and one hash —
        # the property the typed client/server comparison is built on.
        self.assertEqual(
            service_meta.SCHEMA_HASH_HEX,
            hashlib.sha256(service_meta.SCHEMA_BFBS).hexdigest(),
        )

        class ForeignMeta:
            SCHEMA_BFBS = service_meta.SCHEMA_BFBS
            SCHEMA_HASH_HEX = "0" * 64
            ROOT_TYPE_NAME = "edu.demo.EduDemoServiceResponse"
            FIXED_SIZE = None
            HAS_AORTA_HEADER = True

        with self.assertRaises(errors.InvalidArgumentError):
            inject_service_schema(None, service_meta, ForeignMeta)

    def test_generated_request_class_uses_the_include_only_enum(self):
        # The enum lives in the include-only `edu_demo_units.fbs`, which is a
        # registered item with every flag false — proof that an include-only
        # file still reaches the generated code through the closure.
        request = EduDemoServiceRequestT()
        request.message = "ping"
        request.unit = EduDemoUnit.SECONDS
        builder = flatbuffers.Builder(64)
        builder.Finish(request.Pack(builder))
        decoded = EduDemoServiceRequest.GetRootAs(bytes(builder.Output()), 0)
        self.assertEqual(b"ping", decoded.Message())
        self.assertEqual(EduDemoUnit.SECONDS, decoded.Unit())


class ActionCellTest(unittest.TestCase):
    """G3 cell: Python × action — the generic SDK codec over the EXTERNAL
    root's generated object-API classes (the action must really go through
    the object API / codec, not stop at "the tree built")."""

    def setUp(self):
        self.codec = FlatbuffersActionCodec(
            goal=EduDemoJobGoalT,
            result=EduDemoJobResultT,
            feedback=EduDemoJobFeedbackDataT,
        )

    def test_goal_payload_round_trips_through_the_codec(self):
        goal = EduDemoJobGoalT()
        goal.target = "dock"
        goal.repeats = 3

        # Client side builds the wrapper; server side decodes it back into the
        # generated object-API class.
        request = self.codec.build_send_goal_request(
            "goal-1", FlatbuffersActionCodec.serialize(goal)
        )
        self.assertEqual("goal-1", self.codec.decode_send_goal_request_goal_id(request))
        decoded = self.codec.decode_goal(request)
        self.assertEqual(b"dock", decoded.target)
        self.assertEqual(3, decoded.repeats)

    def test_result_payload_is_spliced_as_a_nested_table(self):
        result = EduDemoJobResultT()
        result.durationMs = 1234
        result.completed = 3
        encoded = self.codec.encode_get_result_response(
            "goal-1",
            int(ActionStatus.SUCCEEDED),
            1,
            2,
            3,
            0,
            b"",
            FlatbuffersActionCodec.serialize(result),
            int(ActionReplyCode.OK),
            "",
            "epoch-1",
        )

        # (a) the GENERATED wrapper class reads what the SDK's generic codec
        #     wrote — the external root's codegen and the SDK agree on the wire
        #     layout, which is the whole point of the action cell;
        response = EduDemoJobGetResultResponse.GetRootAs(encoded, 0)
        self.assertEqual(b"goal-1", response.GoalId())
        self.assertEqual(1234, response.Result().DurationMs())
        self.assertEqual(3, response.Result().Completed())

        # (b) and the codec parses its own encoding back through the object API.
        parsed = self.codec.parse_get_result_response(encoded)
        self.assertEqual("goal-1", parsed.goal_id)
        result_back = EduDemoJobResultT.InitFromPackedBuf(parsed.result, 0)
        self.assertEqual(1234, result_back.durationMs)
        self.assertEqual(3, result_back.completed)

    def test_action_schema_meta_is_the_aggregate_identity(self):
        self.assertEqual(64, len(job_meta.SCHEMA_HASH_HEX))
        self.assertEqual(
            job_meta.SCHEMA_HASH_HEX,
            hashlib.sha256(job_meta.SCHEMA_BFBS).hexdigest(),
        )
        self.assertEqual("edu.demo.EduDemoJobGoal", job_meta.ROOT_TYPE_NAME)


if __name__ == "__main__":
    unittest.main()
