import unittest
from unittest.mock import Mock, mock_open, patch

import ploomes.ploomes_move_duplicate_deals as move_module


def _mock_response(status_code: int, text: str = "") -> Mock:
    response = Mock()
    response.status_code = status_code
    response.text = text
    return response


class TestMoveDealStatusMapping(unittest.TestCase):
    def test_move_deal_returns_expected_status_for_http_codes(self):
        cases = [
            (200, "ok"),
            (204, "ok"),
            (404, "not_found"),
            (401, "error"),
            (403, "error"),
        ]

        for status_code, expected_status in cases:
            with self.subTest(status_code=status_code):
                logger = Mock()

                with (
                    patch.object(move_module._rate_limiter, "acquire") as mock_acquire,
                    patch.object(
                        move_module.requests,
                        "patch",
                        return_value=_mock_response(status_code),
                    ) as mock_patch,
                    patch.object(move_module.time, "sleep") as mock_sleep,
                ):
                    deal_id, result_status = move_module._move_deal(12345, logger)

                self.assertEqual(deal_id, 12345)
                self.assertEqual(result_status, expected_status)
                self.assertEqual(mock_patch.call_count, 1)
                self.assertEqual(mock_acquire.call_count, 1)
                mock_sleep.assert_not_called()

    def test_move_deal_retries_on_429_then_succeeds(self):
        logger = Mock()

        with (
            patch.object(move_module._rate_limiter, "acquire") as mock_acquire,
            patch.object(
                move_module.requests,
                "patch",
                side_effect=[_mock_response(429), _mock_response(204)],
            ) as mock_patch,
            patch.object(move_module.time, "sleep") as mock_sleep,
            patch.object(move_module.random, "uniform", return_value=0),
        ):
            deal_id, result_status = move_module._move_deal(98765, logger)

        self.assertEqual(deal_id, 98765)
        self.assertEqual(result_status, "ok")
        self.assertEqual(mock_patch.call_count, 2)
        self.assertEqual(mock_acquire.call_count, 2)
        mock_sleep.assert_called_once()


class TestFetchFilterBehavior(unittest.TestCase):
    def test_build_fetch_filter_all_mode_excludes_trash_without_creator(self):
        with patch.object(move_module, "CREATOR_FILTER_MODE", "all"):
            result = move_module._build_fetch_filter(110067326)

        self.assertIn("PipelineId eq 110067326", result)
        self.assertIn(
            f"StageId ne {move_module.TRASH_PIPELINE_STAGE_ID}",
            result,
        )
        self.assertNotIn("CreatorId eq", result)

    def test_build_fetch_filter_creator_only_mode_includes_creator(self):
        with (
            patch.object(move_module, "CREATOR_FILTER_MODE", "creator_only"),
            patch.object(move_module, "CREATOR_ID", "110034764"),
        ):
            result = move_module._build_fetch_filter(110066424)

        self.assertIn("PipelineId eq 110066424", result)
        self.assertIn(
            f"StageId ne {move_module.TRASH_PIPELINE_STAGE_ID}",
            result,
        )
        self.assertIn("CreatorId eq 110034764", result)


class TestFetchPageRequest(unittest.TestCase):
    def test_fetch_page_sends_expected_filter_parameter(self):
        logger = Mock()
        response = Mock()
        response.status_code = 200
        response.json.return_value = {"value": []}

        with (
            patch.object(
                move_module, "_build_fetch_filter", return_value="EXPECTED_FILTER"
            ),
            patch.object(move_module._rate_limiter, "acquire") as mock_acquire,
            patch.object(
                move_module.requests, "get", return_value=response
            ) as mock_get,
        ):
            deals = move_module._fetch_page(skip=0, pipeline_id=123, logger=logger)

        self.assertEqual(deals, [])
        self.assertEqual(mock_acquire.call_count, 1)
        _, kwargs = mock_get.call_args
        self.assertEqual(kwargs["params"]["$filter"], "EXPECTED_FILTER")


class TestMainIdempotencyGuard(unittest.TestCase):
    def test_main_does_not_move_deals_already_in_trash_stage(self):
        logger = Mock()
        report_manager_instance = Mock()
        report_manager_instance.get_full_path.return_value = "dummy_audit.csv"

        deals_page = [
            {
                "Id": 1,
                "StageId": 999,
                "CreateDate": "2026-04-10T10:00:00Z",
                "OtherProperties": [
                    {"FieldKey": move_module.CNJ_FIELD_KEY, "StringValue": "CNJ-1"},
                    {
                        "FieldKey": move_module.PRODUCT_FIELD_KEY,
                        "StringValue": "PROD-1",
                    },
                ],
            },
            {
                "Id": 2,
                "StageId": 999,
                "CreateDate": "2026-04-10T11:00:00Z",
                "OtherProperties": [
                    {"FieldKey": move_module.CNJ_FIELD_KEY, "StringValue": "CNJ-1"},
                    {
                        "FieldKey": move_module.PRODUCT_FIELD_KEY,
                        "StringValue": "PROD-1",
                    },
                ],
            },
            {
                "Id": 3,
                "StageId": move_module.TRASH_PIPELINE_STAGE_ID,
                "CreateDate": "2026-04-10T12:00:00Z",
                "OtherProperties": [
                    {"FieldKey": move_module.CNJ_FIELD_KEY, "StringValue": "CNJ-1"},
                    {
                        "FieldKey": move_module.PRODUCT_FIELD_KEY,
                        "StringValue": "PROD-1",
                    },
                ],
            },
        ]

        with (
            patch.object(move_module, "PIPELINE_IDS", [110067326]),
            patch.object(move_module, "DRY_RUN", False),
            patch.object(move_module, "setup_logging", return_value=logger),
            patch.object(
                move_module, "ReportManager", return_value=report_manager_instance
            ),
            patch.object(move_module, "_fetch_page", return_value=deals_page),
            patch.object(
                move_module, "_move_deal", return_value=(2, "ok")
            ) as mock_move,
            patch("builtins.open", mock_open()),
        ):
            move_module.main()

        self.assertEqual(mock_move.call_count, 1)
        moved_deal_id = mock_move.call_args.args[0]
        self.assertEqual(moved_deal_id, 2)


if __name__ == "__main__":
    unittest.main()
