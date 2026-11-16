"""取消 → 仪器交接链路的回归测试（可控交错）。

覆盖两条修复：
1. A 的取消停机工作（带外 ABORt）不得延迟到 B 的扫描上执行——
   用“闸门”按住取消派生的 ABORt 任务，验证 B 在闸门放开前不会启动，
   且放行后迟到的 ABORt 落在 A 的执行槽内，B 正常完成。
2. 取消期间仪器不可达：A 仍按取消收口（读数不保存），但必须标记未同步，
   B 保持排队，期间查询可用；仪器恢复后重同步完成，B 自动开始采集。
"""

from __future__ import annotations

import asyncio
import threading
import time


async def _wait_db(db, batch_id, targets, timeout=10.0):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = await db.get_batch(batch_id)
        if last and last["status"] in targets:
            return last
        await asyncio.sleep(0.05)
    raise AssertionError(
        f"批次 {batch_id} 未在 {timeout}s 内到达 {targets}，最后状态 "
        f"{last and last['status']}")


def _wait_status(client, batch_id, targets, timeout=10.0):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = client.get(f"/batches/{batch_id}").json()
        if last["status"] in targets:
            return last
        time.sleep(0.05)
    raise AssertionError(
        f"批次 {batch_id} 未在 {timeout}s 内到达 {targets}，最后状态 {last}")


def test_late_cancel_abort_is_contained_before_next_batch(api_components):
    """可控交错：按住 A 的取消 ABORt，验证它不会漏到 B 的扫描上。"""
    portal, _settings, db, scpi, coordinator = api_components

    async def scenario():
        await scpi.set_delay(0.05)
        await db.connect()
        await coordinator.startup()

        gate = asyncio.Event()
        abort_started = asyncio.Event()
        real_abort = coordinator._abort_for_cancel

        async def gated_abort(batch_id):
            abort_started.set()
            await gate.wait()  # 可控交错点：延迟 A 的带外 ABORt
            await real_abort(batch_id)

        coordinator._abort_for_cancel = gated_abort
        try:
            await db.create_batch("B-A", [101, 102])
            await coordinator.enqueue("B-A")
            await _wait_db(db, "B-A", {"RUNNING"})

            # 取消接口及时受理
            result = await coordinator.request_cancel("B-A")
            assert result["accepted"] is True
            assert result["status"].value == "CANCELLING"
            await asyncio.wait_for(abort_started.wait(), timeout=2)

            await db.create_batch("B-B", [103])
            await coordinator.enqueue("B-B")
            await asyncio.sleep(0.6)

            # 闸门未放开：A 的停机工作未完结，B 不得启动
            row_a = await db.get_batch("B-A")
            row_b = await db.get_batch("B-B")
            assert row_a["status"] == "CANCELLED"
            assert row_b["status"] == "QUEUED"

            # 放行：迟到的 ABORt 落在 A 的执行槽内（仪器已空闲，无害），
            # B 的扫描不受影响，正常完成并取到自己的读数
            gate.set()
            row_b = await _wait_db(db, "B-B", {"COMPLETED"}, timeout=8)
            assert row_b["error"] is None
            readings = await db.get_readings_for_batch("B-B")
            assert [r["channel"] for r in readings] == [103]
            # A 不保存读数
            assert await db.get_readings_for_batch("B-A") == []
        finally:
            coordinator._abort_for_cancel = real_abort
            gate.set()
            await coordinator.shutdown()
            await db.close()

    portal.call(scenario)


def test_cancel_during_outage_defers_next_batch_until_resync(
        api_client, api_components, instrument_portal):
    """取消期间仪器不可达：A 取消收口但标未同步，B 排队，恢复后自动采集。"""
    from instrument.scpi_device import create_server

    client, _app, port, scpi, portal = api_client
    _portal, _settings, _db, _scpi, coordinator = api_components
    _p, _port, state = instrument_portal

    gate = threading.Event()
    real_abort = coordinator._abort_for_cancel

    async def gated_abort(batch_id):
        # 可控交错点：按住取消派生的 ABORt，直到仪器被关停
        while not gate.is_set():
            await asyncio.sleep(0.02)
        await real_abort(batch_id)

    new_server = None
    coordinator._abort_for_cancel = gated_abort
    try:
        portal.call(scpi.set_delay, 0.4)
        batch_a = client.post(
            "/batches", json={"channels": [101, 102, 103, 104]}).json()
        _wait_status(client, batch_a["id"], {"RUNNING"})

        # 取消接口及时返回受理（不等待仪器）
        t0 = time.monotonic()
        result = client.post(f"/batches/{batch_a['id']}/cancel").json()
        assert time.monotonic() - t0 < 2.0
        assert result["accepted"] is True
        assert result["status"] == "CANCELLING"

        # 仪器不可达：停止监听并断开全部客户端连接，随后放开 ABORt 闸门
        def kill_instrument():
            for conn in list(state.connections):
                conn.alive = False
                try:
                    conn.writer.close()
                except Exception:  # noqa: BLE001
                    pass
            state.server.close()

        portal.call(kill_instrument)
        gate.set()

        # A 按取消收口：CANCELLED 且不保存读数；通信失败标为未同步
        final_a = _wait_status(client, batch_a["id"], {"CANCELLED"}, timeout=10)
        assert final_a["status"] == "CANCELLED"
        assert client.get(
            f"/batches/{batch_a['id']}/readings").json()["readings"] == []

        # 后续批次继续排队；排队取消在仪器不可达期间仍立即可用
        batch_b = client.post("/batches", json={"channels": [103]}).json()
        batch_c = client.post("/batches", json={"channels": [104]}).json()
        cancel_c = client.post(f"/batches/{batch_c['id']}/cancel").json()
        assert cancel_c["accepted"] is True
        assert cancel_c["status"] == "CANCELLED"

        # 停机及迟到响应清理未确认前不得启动 B；期间查询可用
        deadline = time.time() + 1.5
        while time.time() < deadline:
            assert client.get(f"/batches/{batch_b['id']}").json()[
                "status"] == "QUEUED"
            health = client.get("/health").json()
            assert health["instrument_synced"] is False
            assert client.get("/batches").status_code == 200
            assert client.get(
                f"/batches/{batch_a['id']}/readings").status_code == 200
            time.sleep(0.1)

        # 等仪器侧遗留扫描自然结束，再恢复仪器（重绑定同一端口）
        portal.call(lambda: _wait_scan_idle(state))
        new_server, _ = portal.call(create_server, "127.0.0.1", port, state)

        # 恢复后自动重同步并采集：B 完成，C 保持已取消
        final_b = _wait_status(client, batch_b["id"], {"COMPLETED"}, timeout=20)
        assert final_b["error"] is None
        readings = client.get(f"/batches/{batch_b['id']}/readings").json()
        assert [r["channel"] for r in readings["readings"]] == [103]
        assert client.get(f"/batches/{batch_c['id']}").json()[
            "status"] == "CANCELLED"
        assert client.get("/health").json()["instrument_synced"] is True
    finally:
        gate.set()
        coordinator._abort_for_cancel = real_abort
        if new_server is not None:
            portal.call(new_server.close)


async def _wait_scan_idle(state, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not state.scanning:
            return
        await asyncio.sleep(0.05)
    raise AssertionError("仪器遗留扫描未在超时内结束")
