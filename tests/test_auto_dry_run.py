import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, patch

from modes import auto_trader


class AutoDryRunTests(unittest.IsolatedAsyncioTestCase):
    async def test_dry_run_sizes_from_assumed_collateral_without_live_capacity(self):
        with (
            patch.object(auto_trader, "resolve_size_pct_from_active_asset_data", new_callable=AsyncMock) as capacity,
            patch.object(auto_trader, "get_user_state_with_retry", new_callable=AsyncMock) as state,
            patch.object(auto_trader, "get_all_mids", new_callable=AsyncMock, return_value={"SOL": 100.0}),
            patch.object(auto_trader, "post_active_asset_data", new_callable=AsyncMock,
                         side_effect=RuntimeError("Account API unavailable")) as active_asset,
            patch.object(auto_trader, "_info_meta_with_optional_dex", new_callable=AsyncMock,
                         return_value={"universe": [{"name": "SOL", "maxLeverage": 2}]}),
            patch.object(auto_trader, "round_size_for_hyperliquid", new_callable=AsyncMock,
                         side_effect=lambda info, coin, size: size),
        ):
            size, reason = await auto_trader.resolve_auto_trade_size(
                object(), "0x0", "SOL", "long", None, 0.1, dry_run=True,
            )
        self.assertEqual(size, 20.0)
        self.assertIn("collateral=10000.00000000", reason)
        capacity.assert_not_awaited()
        state.assert_not_awaited()
        active_asset.assert_not_awaited()
        self.assertIn("leverage_source=meta.universe.maxLeverage", reason)

    async def test_live_leverage_uses_account_setting(self):
        with (
            patch.object(auto_trader, "post_active_asset_data", new_callable=AsyncMock,
                         return_value={"leverage": {"value": "3"}}) as active_asset,
            patch.object(auto_trader, "_info_meta_with_optional_dex", new_callable=AsyncMock) as meta,
        ):
            result = await auto_trader.get_instrument_leverage_for_size_pct(object(), "0x0", "SOL")
        self.assertEqual(result, (Decimal("3"), "activeAssetData.leverage.value"))
        active_asset.assert_awaited_once()
        meta.assert_not_awaited()

    async def test_dry_snapshot_does_not_require_live_account(self):
        with (
            patch.object(auto_trader, "get_user_state_with_retry", new_callable=AsyncMock,
                         side_effect=RuntimeError("Account API unavailable")) as state,
            patch.object(auto_trader, "get_user_fills_since", new_callable=AsyncMock,
                         side_effect=RuntimeError("Account API unavailable")) as fills,
            patch.object(auto_trader, "get_all_mids", new_callable=AsyncMock, return_value={"SOL": 100.0}),
        ):
            snapshot = await auto_trader.build_auto_scan_loop_snapshot(object(), "0x0", 0, dry_run=True)
        self.assertEqual(snapshot.account_balance, 10_000.0)
        self.assertEqual(snapshot.positions_by_coin, {})
        self.assertEqual(snapshot.realized_pnl, 0.0)
        self.assertEqual(snapshot.mids, {"SOL": 100.0})
        state.assert_not_awaited()
        fills.assert_not_awaited()

    async def test_live_mode_still_uses_account_capacity(self):
        with patch.object(auto_trader, "resolve_size_pct_from_active_asset_data",
                          new_callable=AsyncMock, return_value=(3.0, "live capacity")) as capacity:
            result = await auto_trader.resolve_auto_trade_size(
                object(), "0x0", "SOL", "long", None, 0.1,
            )
        self.assertEqual(result, (3.0, "live capacity"))
        capacity.assert_awaited_once()

    async def test_fixed_size_is_preserved_in_dry_run(self):
        result = await auto_trader.resolve_auto_trade_size(
            object(), "0x0", "SOL", "long", 7.0, None, dry_run=True,
        )
        self.assertEqual(result[0], 7.0)

    async def test_snapshot_shows_assumed_balance_only_in_dry_run(self):
        with (
            patch.object(auto_trader, "get_user_state_with_retry", new_callable=AsyncMock,
                         return_value={"marginSummary": {"accountValue": "0"}, "assetPositions": []}),
            patch.object(auto_trader, "get_all_mids", new_callable=AsyncMock, return_value={"SOL": 100.0}),
            patch.object(auto_trader, "get_user_fills_since", new_callable=AsyncMock, return_value=[]),
        ):
            dry_snapshot = await auto_trader.build_auto_scan_loop_snapshot(object(), "0x0", 0, dry_run=True)
            live_snapshot = await auto_trader.build_auto_scan_loop_snapshot(object(), "0x0", 0)
        self.assertEqual(dry_snapshot.account_balance, 10_000.0)
        self.assertEqual(live_snapshot.account_balance, 0.0)
